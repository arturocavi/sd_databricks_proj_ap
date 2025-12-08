"""
Silver Layer - Accounts Payable Invoices Transformation Pipeline
======================================================================
This script performs data transformation on the 'ap.bronze.ap_invoices' table.

Steps:
    1. Standardize date formats
    2. Clean text and ID fields
    3. Create an enriched dataset with calculated fields:
        - POAmount
        - OnTimePayment
        - AgingDays
        - Month

Assumptions:
    - 'today' is the latest date found in 'invoice_date' or 'paid_date'.
    - If 'paid_date' is blank, the invoice is considered unpaid.
    - If unpaid and 'due_date' > 'today', the invoice is not considered overdue.
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import BooleanType, IntegerType, DateType
from pyspark.sql.window import Window
from datetime import datetime
import re
import argparse


# ============================================================================
# CONFIGURATION
# ============================================================================
# Get parameter value for catalog name (target workspace)
parser = argparse.ArgumentParser()
parser.add_argument('--catalog_name', type=str, required=True)
args = parser.parse_args()
CATALOG = args.catalog_name

INPUT_TABLE = f"{CATALOG}.bronze.ap_invoices"
OUTPUT_TABLE = f"{CATALOG}.silver.ap_invoices"
DATE_COLS = ["invoice_date", "due_date", "paid_date"]
TEXT_FIELDS = ["category"]
ID_FIELDS = ["invoice_id", "cost_center", "poid"]


# ============================================================================
# FUNCTIONS
# ============================================================================
def parse_date_flexible(date_str):
    """
    Parse date string with multiple format attempts.
    """
    if date_str is None or date_str.strip() == "":
        return None
    
    date_str = date_str.strip()
    
    # List of formats to try
    formats = [
        "%m/%d/%Y",  # 07/01/2025
        "%m/%d/%y",  # 07/01/25
        "%-m/%d/%Y", # 7/01/2025 (Unix-like systems)
        "%-m/%d/%y", # 7/01/25
        "%-m/%-d/%Y", # 7/1/2025
        "%-m/%-d/%y", # 7/1/25
        "%Y-%m-%d",  # 2025-07-01
        "%d/%m/%Y",  # 01/07/2025
    ]
    
    # Remove leading zeros: "07/01/25" -> "7/1/25"
    normalized = re.sub(r'\b0(\d)', r'\1', date_str)
    
    for fmt in formats:
        for test_str in [date_str, normalized]:
            try:
                # Remove %-  for Windows (doesn't support it)
                fmt_clean = fmt.replace('%-', '%')
                return datetime.strptime(test_str, fmt_clean).date()
            except (ValueError, AttributeError):
                continue
    
    return None

# Register UDF
parse_date_udf = udf(parse_date_flexible, DateType())

def standardize_dates_udf(df, columns):
    """Use UDF for more flexible parsing"""
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, parse_date_udf(col(col_name)))
    return df


def clean_text_fields(df: DataFrame, text_cols: list, id_cols: list) -> DataFrame:
    """Clean text and ID fields: trim spaces, normalize casing."""
    
    # Clean text fields (trim and title case)
    for col_name in text_cols:
        df = df.withColumn(
            col_name,
            initcap(trim(col(col_name)))
        )
    
    # Clean ID fields (trim and upper case)
    for col_name in id_cols:
        df = df.withColumn(
            col_name,
            upper(trim(col(col_name)))
        )
    
    return df


def enrich_data(df: DataFrame) -> DataFrame:
    """Add calculated columns: po_amount, on_time_payment, aging_days, month."""
    
    # Compute po_amount
    df = df.withColumn("po_amount", col("quantity") * col("unit_price_po"))
    
    # Define 'today' as the latest date from invoice_date or paid_date
    today = df.select(
        greatest(
            max("invoice_date"),
            max("paid_date")
        ).alias("today")
    ).first()["today"]
    
    # Create on_time_payment column
    df = df.withColumn(
        "on_time_payment",
        when(
            col("paid_date").isNotNull() & (col("paid_date") <= col("due_date")),
            lit(True)
        ).when(
            col("paid_date").isNotNull() & (col("paid_date") > col("due_date")),
            lit(False)
        ).when(
            col("paid_date").isNull() & (col("due_date") <= lit(today)),
            lit(False)
        ).otherwise(lit(None)).cast(BooleanType())
    )
    
    # Create Aginaging_daysgDays column
    df = df.withColumn(
        "aging_days",
        when(
            col("paid_date").isNull(),
            datediff(lit(today), col("invoice_date"))
        ).otherwise(
            datediff(col("paid_date"), col("invoice_date"))
        ).cast(IntegerType())
    )
    
    # Create month column
    df = df.withColumn(
        "month",
        date_format(col("invoice_date"), "yyyy-MM")
    )
    
    return df

def drop_ingest_time(df: DataFrame) -> DataFrame:
    """Drop the 'ingest_time' column if it exists."""
    if "ingest_time" in df.columns:
        return df.drop("ingest_time")
    return df


# ============================================================================
# TABLE CREATION
# ============================================================================
# Execute the data transformation pipeline.

print(f"Loading source data from {INPUT_TABLE}...")
df = spark.table(INPUT_TABLE)

print("Standardizing date formats...")
df = standardize_dates_udf(df, DATE_COLS)

print("Cleaning text and ID fields...")
df = clean_text_fields(df, TEXT_FIELDS, ID_FIELDS)

print("Enriching data with calculated columns...")
df = enrich_data(df)

print("Dropping 'ingest_time' column...")
df = drop_ingest_time(df)

print(f"Writing enriched data to {OUTPUT_TABLE}...")
df.write.format("delta").mode("overwrite").saveAsTable(OUTPUT_TABLE)

print("Transformation pipeline completed successfully!")
