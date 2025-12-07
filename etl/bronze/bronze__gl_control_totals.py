"""
Bronze layer - GL Control Totals
===================================

"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
from pyspark.sql.functions import current_timestamp, to_timestamp_ntz 

# ============================================================================
# CONFIGURATION
# ============================================================================
# Storage configuration
STORAGE_ACCOUNT = "sd0212"
CONTAINER = "bronze"
CATALOG = "ap"
SCHEMA = "bronze"
TABLE = "gl_control_totals"


# ============================================================================
# TABLE CREATION
# ============================================================================
# Path to storage
path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{TABLE}"

# Read data from storage
df = (
    spark
    .read
    .format("delta")
    .option("header", True)
    .load(path)
    .withColumn("ingest_time", to_timestamp_ntz(current_timestamp()))
    .drop("_rescued_data")
)

# Renaming columns to lowercase
df = (
    df
    .toDF(*[c.lower() for c in df.columns])
)

# Write data to table
(
    df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
)


# Display table
# spark.read.table(f"{CATALOG}.{SCHEMA}.{TABLE}").display()