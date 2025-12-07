"""
Data Reading Module
===================================
Staging data from source to bronze

This module automates the ingestion of raw data from a source container to a bronze layer in Azure Data Lake using Databricks Auto Loader and Structured Streaming.
It is designed to process multiple tables with configurable formats and schema hints, supporting both CSV and JSON sources.
The script reads data incrementally from the source, writes it as Delta tables to the bronze container, and manages schema evolution and checkpointing for reliable, idempotent ingestion.
Logging is included for monitoring and error handling.
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
import logging
from pyspark.sql.streaming import StreamingQuery

# ============================================================================
# CONFIGURATION
# ============================================================================
# Storage configuration
STORAGE_ACCOUNT = "sd0212"
CHECKPOINT_CONTAINER = "bronze"
SOURCE_CONTAINER = "source"

# Tables configuration
TABLES = ["ap_invoices", "suppliers", "gl_control_totals"]

SCHEMA_HINTS = {
    "ap_invoices": "InvoiceID STRING, InvoiceDate STRING, DueDate STRING, PaidDate STRING, SupplierID INT, Category STRING, CostCenter STRING, InvoiceAmount DOUBLE, Currency STRING, POID STRING, Quantity INT, UnitPrice_PO DOUBLE, UnitPrice_Invoice DOUBLE",
    "suppliers": "SupplierID INT, Supplier STRING",
    "gl_control_totals": "GL_Approved_Spend DOUBLE, Month STRING"
}

FORMATS = {
    "ap_invoices": "csv",
    "suppliers": "csv",
    "gl_control_totals": "json"
}

# Logging configuration
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)

# ============================================================================
# FUNCTION
# ============================================================================
def auto_load_data(storage_account, checkpoint_container, source_container, table, file_format, schema_hint):
    """
    Auto-load data from source container to bronze container using Structured Streaming.
    
    Args:
        storage_account: Azure storage account name
        checkpoint_container: Container for checkpoints and output data
        source_container: Container with source data
        table: Table name to process
        file_format: File format (csv, json, etc.)
        schema_hint: Schema hint string for the table
    
    Returns:
        StreamingQuery: The streaming query object
    """
    try:
        checkpoint_path = f"abfss://{checkpoint_container}@{storage_account}.dfs.core.windows.net/checkpoint_{table}"
        source_path = f"abfss://{source_container}@{storage_account}.dfs.core.windows.net/{table}"
        output_path = f"abfss://{checkpoint_container}@{storage_account}.dfs.core.windows.net/{table}"
        
        logging.info(f"Starting data load for table: {table}")
        
        # Read stream with Auto Loader
        df = (
            spark
            .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .option("cloudFiles.schemaLocation", checkpoint_path)
            .option("cloudFiles.schemaHints", schema_hint)
            .option("cloudFiles.inferColumnTypes", "true")
            .load(source_path)
        )   
        
        # Write stream 
        query = (
            df
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .option("path", output_path)
            .trigger(availableNow=True)
            .start()
        )
        
        # Wait for the stream to complete (since using availableNow trigger)
        query.awaitTermination()
        
        logging.info(f"Data loading for table {table} completed successfully.")
        return query
        
    except Exception as e:
        logging.error(f"Error processing table {table}: {str(e)}")
        raise

# ============================================================================
# EXECUTION
# ============================================================================

for table in TABLES:
    try:
        # Pass the specific format and schema hint for each table
        file_format = FORMATS[table]
        schema_hint = SCHEMA_HINTS[table]
        
        auto_load_data(
            storage_account=STORAGE_ACCOUNT,
            checkpoint_container=CHECKPOINT_CONTAINER,
            source_container=SOURCE_CONTAINER,
            table=table,
            file_format=file_format,
            schema_hint=schema_hint
        )
        
    except Exception as e:
        logging.error(f"Failed to process table {table}: {str(e)}")
        break     # Stop processing on first error
