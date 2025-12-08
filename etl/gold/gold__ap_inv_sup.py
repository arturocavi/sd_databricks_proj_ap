"""
Gold Layer - Accounts Payable & Supplier Join
======================================================================
This script performs the join between the silver layer tables 'ap_invoices' and 'suppliers' to create the gold layer table 'ap_inv_sup'.

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

INPUT_INVOICES = f"{CATALOG}.silver.ap_invoices"
INPUT_SUPPLIERS = f"{CATALOG}.silver.suppliers"
OUTPUT_TABLE = f"{CATALOG}.gold.ap_inv_sup"


# ============================================================================
# TABLE CREATION
# ============================================================================
# Read tables to join
df_invoices = spark.table(INPUT_INVOICES)
df_suppliers = spark.table(INPUT_SUPPLIERS)

# Join tables
df_ap = (
    df_invoices
    .join(
        df_suppliers, 
        on= df_invoices.supplier_id == df_suppliers.supplier_id,
        how='inner'
    )
    .drop(df_suppliers.supplier_id)
    .drop(df_invoices.supplier_id)
)

# Reorder columns
cols = df_ap.columns
cols.remove('supplier')
cols.insert(4, 'supplier')
df_ap = df_ap.select(*cols)

# Write table
df_ap.write.mode("overwrite").saveAsTable(OUTPUT_TABLE)