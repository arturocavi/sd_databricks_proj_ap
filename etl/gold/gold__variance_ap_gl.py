"""
Gold Layer - Variance Check of AP Total Spend vs GL Approved Spend
=====================================================================================
This script helps to identify discrepancies between AP records and GL control totals.

"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
import argparse


# ============================================================================
# CONFIGURATION
# ============================================================================
# Get parameter value for catalog name (target workspace)
parser = argparse.ArgumentParser()
parser.add_argument('--catalog_name', type=str, required=True)
args = parser.parse_args()
CATALOG = args.catalog_name

INPUT_GL = f"{CATALOG}.silver.gl_control_totals"
INPUT_INVOICES = f"{CATALOG}.silver.ap_invoices"
OUTPUT_TABLE = f"{CATALOG}.gold.variance_ap_gl"


# ============================================================================
# TABLE CREATION
# ============================================================================
from pyspark.sql.functions import sum, round, col

# Read the tables to check
df_gl = spark.table(INPUT_GL)
df_invoices = spark.table(INPUT_INVOICES)

# Calculate actual spend
df_ap_agg = (
    df_invoices
    .groupBy('month')
    .agg(
        round(sum('invoice_amount'),2)
        .alias('actual_spend')
    )
    .orderBy('month')
)

# Calculate the variance
df_variance = (
    df_ap_agg
    .join(df_gl, on='month', how='inner')
    .withColumn("difference", col("actual_spend") - col("gl_approved_spend"))
)

# Write table
df_variance.write.mode("overwrite").saveAsTable(OUTPUT_TABLE)