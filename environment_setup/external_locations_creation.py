"""
External Locations Creation Module
===================================
This module creates external locations in Databricks Unity Catalog for Azure Data Lake Storage containers.
"""

# ============================================================================
# DEPENDENCIES
# ============================================================================
import logging
from pyspark.sql.utils import AnalysisException


# ============================================================================
# CONFIGURATION
# ============================================================================
# Storage configuration
CONTAINERS = ["source", "bronze", "silver", "gold"]
STORAGE_ACCOUNT = "sd0212"
STORAGE_CREDENTIAL = "sd0212_credential"

# Logging configuration
logging.basicConfig(
    level=logging.INFO, 
    format='%(levelname)s:%(name)s:%(message)s'
)


# ============================================================================
# FUNCTION
# ============================================================================
def create_external_locations(containers, storage_account, storage_credential="adb_access_con"):
    """
    Create external locations in Unity Catalog for specified containers.
    
    This function creates external locations for each container in an Azure Data Lake 
    Storage Gen2 account. It handles common errors such as hostname resolution failures 
    and cloud storage access issues. If a hostname resolution error occurs, the process 
    stops immediately as subsequent attempts would fail.
    
    Parameters
    ----------
    containers : list of str
        List of container names for which to create external locations.
    storage_account : str
        Name of the Azure Data Lake Storage Gen2 account.
    storage_credential : str, optional
        Name of the storage credential in Unity Catalog (default: "adb_access_con").
    
    Returns
    -------
    None
    
    Raises
    ------
    None
        Errors are logged but not raised to allow partial completion.
    
    Examples
    --------
    >>> mycontainers = ["source", "bronze", "silver", "gold"]
    >>> mystorageaccount = "storageaccountname"
    >>> create_external_locations(mycontainers, "mystorageaccount")
    
    Notes
    -----
    - The function uses CREATE EXTERNAL LOCATION IF NOT EXISTS to be idempotent
    - Hostname resolution failures cause immediate process termination
    - Other errors are logged but allow processing to continue for remaining containers
    """
    for container_name in containers:
        try:
            spark.sql(f"""
                CREATE EXTERNAL LOCATION IF NOT EXISTS `{container_name}_ext_loc`
                URL 'abfss://{container_name}@{storage_account}.dfs.core.windows.net/'
                WITH (STORAGE CREDENTIAL {storage_credential})
                COMMENT 'External Location for the {container_name} container at the {storage_account} ADLS account';
            """)
            logging.info(
                f"External location `{container_name}_ext_loc` created (or already exists)"
            )
        
        except AnalysisException as ae:
            # Extract only the first line of the error (without stack trace)
            error_msg = str(ae).split('\n')[0]
            
            if "Cannot resolve hostname" in error_msg or "Cannot resolve" in error_msg:
                logging.error(
                    f"AnalysisException for container '{container_name}': Hostname resolution failed."
                )
                logging.critical(
                    f"Stopping process due to hostname resolution failure. "
                    f"Please verify the storage account name: '{storage_account}'."
                )
                break  # Stop the loop if there's a hostname resolution failure
            
            elif any(keyword in error_msg for keyword in [
                "Failed to access cloud storage", 
                "AbfsRestOperationException", 
                "UC_CLOUD_STORAGE_ACCESS_FAILURE"
            ]):
                logging.error(
                    f"AnalysisException for container '{container_name}': Cloud storage access failure. "
                    f"This usually means the container name is invalid or the credential lacks access."
                )
            else:
                logging.error(
                    f"AnalysisException creating external location for '{container_name}': {error_msg}"
                )
        
        except Exception as e:
            # Extract only the first line of the error (without stack trace)
            error_msg = str(e).split('\n')[0]
            logging.error(
                f"Unexpected error when creating external location for '{container_name}': {error_msg}"
            )
    
    logging.info("External locations creation process finished.")


# ============================================================================
# EXECUTION
# ============================================================================
create_external_locations(
    containers=CONTAINERS,
    storage_account=STORAGE_ACCOUNT,
    storage_credential=STORAGE_CREDENTIAL
)
