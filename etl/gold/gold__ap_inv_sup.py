"""
Gold Layer - Accounts Payable & Supplier Join
======================================================================
This script performs the join between the silver layer tables 'ap_invoices' and 'suppliers' to create the gold layer table 'ap_inv_sup'.

"""

# ============================================================================
# TABLE CREATION
# ============================================================================
# Read tables to join
df_invoices = spark.table("ap.silver.ap_invoices")
df_suppliers = spark.table("ap.silver.suppliers")

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
df_ap.write.mode("overwrite").saveAsTable("ap.gold.ap_inv_sup")