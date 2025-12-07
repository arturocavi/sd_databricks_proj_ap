"""
Gold Layer - Variance Check of AP Total Spend vs GL Approved Spend
=====================================================================================
This script helps to identify discrepancies between AP records and GL control totals.

"""

# ============================================================================
# TABLE CREATION
# ============================================================================
from pyspark.sql.functions import sum, round, col

# Read the tables to check
df_gl = spark.table("ap.silver.gl_control_totals")
df_invoices = spark.table("ap.silver.ap_invoices")

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
df_variance.write.mode("overwrite").saveAsTable("ap.gold.variance_ap_gl")