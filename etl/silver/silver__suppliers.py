"""
Silver Layer - Suppliers
======================================================================
This script performs data transformation on the 'ap.bronze.suppliers' table.

"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
from pyspark.sql.functions import *
import argparse


# ============================================================================
# CONFIGURATION
# ============================================================================
# Get parameter value for catalog name (target workspace)
parser = argparse.ArgumentParser()
parser.add_argument('--catalog_name', type=str, required=True)
args = parser.parse_args()
CATALOG = args.catalog_name

INPUT_TABLE = f"{CATALOG}.bronze.suppliers"
OUTPUT_TABLE = f"{CATALOG}.silver.suppliers"
TEXT_FIELDS = ["supplier"]


# ============================================================================
# FUNCTIONS
# ============================================================================
def clean_text_fields(df: DataFrame, text_cols: list) -> DataFrame:
    """Clean text fields: trim spaces and title case."""
    for col_name in text_cols:
        df = df.withColumn(
            col_name,
            initcap(trim(col(col_name)))
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

print("Cleaning text fields...")
df = clean_text_fields(df, TEXT_FIELDS)

print("Dropping 'ingest_time' column...")
df = drop_ingest_time(df)

print(f"Writing cleaned data to {OUTPUT_TABLE}...")
df.write.format("delta").mode("overwrite").saveAsTable(OUTPUT_TABLE)

print("Transformation pipeline completed successfully!")
