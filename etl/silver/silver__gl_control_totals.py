"""
Silver Layer - GL Control Totals
======================================================================
This script performs data transformation on the 'ap.bronze.gl_control_totals' table.

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

INPUT_TABLE = f"{CATALOG}.bronze.gl_control_totals"
OUTPUT_TABLE = f"{CATALOG}.silver.gl_control_totals"


# ============================================================================
# FUNCTIONS
# ============================================================================
def rename_month_column(df: DataFrame) -> DataFrame:
    """Rename the 'Month' column to 'month' if it exists."""
    if "Month" in df.columns:
        return df.withColumnRenamed("Month", "month")
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

print("Renaming 'Month' column to 'month'...")
df = rename_month_column(df)

print("Dropping 'ingest_time' column...")
df = drop_ingest_time(df)

print(f"Writing cleaned data to {OUTPUT_TABLE}...")
df.write.format("delta").mode("overwrite").saveAsTable(OUTPUT_TABLE)

print("Transformation pipeline completed successfully!")
