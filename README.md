# sd_databricks_proj_ap

Smart Data Databricks project for Accounts Payable (AP) data pipeline
orchestration.

## Overview

This repository contains the codebase for an automated Accounts Payable
pipeline built on Databricks.\
It includes ingestion, transformation, validation, versioning, security
configuration, and dashboarding components to support reliable AP
analytics and reporting.

## 📁 Repository Structure

    .
    ├── environment_setup/      # Environment configuration and setup scripts
    ├── etl/                    # ETL jobs for ingestion, processing, and cleaning of AP data
    ├── explorations/           # Personal Notebooks for exploration and validation (they are not necessary)
    ├── reversion/              # Versioning and rollback utilities
    ├── security/               # Security, credential, and access-control related scripts
    ├── dashboard/              # Dashboard or reporting notebooks/scripts
    ├── .github/workflows/      # Automation & CI/CD workflows
    └── README.md               # Project documentation


## 🔐 Databricks Service Principal Permissions

*Note on why I used a service principal for this project:* \
During the setup of this project, my Databricks workspace incorrectly detected my Azure user account as a Service Principal (SP) instead of a regular human user.
Because of this, Databricks did not allow me to create external locations or storage credentials unless I first granted the required privileges to my own account.

To address this scenario, the **security/grant_privileges_sp.sql** script shows the minimal permissions a Service Principal needs to run this ETL pipeline, create external locations, and access ADLS storage.


## Environment Setup

### 🗂️ External Locations Creation Module
This module automates the creation of External Locations in Databricks Unity Catalog for the project’s Azure Data Lake Storage (ADLS) containers.
It ensures that all required containers (source, bronze, silver, gold) are properly registered in Unity Catalog so they can be used by the ETL pipelines with the correct security and permissions.

🔧 What this module does:

- Reads a list of ADLS containers (CONTAINERS) and the ADLS storage account name.
- Automatically creates the corresponding external locations ensuring the script is idempotent (safe to run multiple times) using:
``` sql
CREATE EXTERNAL LOCATION IF NOT EXISTS
```
- Attaches each location to the specified Unity Catalog storage credential.
- Provides detailed logging for visibility and debugging.

### 🏷️ Catalog & Schemas Creation Notebook
This notebook is intended to:
- Automatically create the necessary catalogs and schemas for your project within Unity Catalog.
- Provide a clean and reproducible setup, so you don’t have to manually create catalogs/schemas through the UI.
- Ensure proper namespace organization right from project initialization (e.g. catalog for the project, schemas for layers such as source, bronze, silver, gold, or dev/production, depending on your target).
- Simplify object creation when running jobs and environment consistency across workspaces.


## 🔄 ETL Pipeline Job Notes

### 📥 Staging Module — Source → Bronze Ingestion
This module in etl/staging_ingestion.py handles the automated ingestion of raw data from the source container to the bronze layer in Azure Data Lake using Databricks Auto Loader and Structured Streaming.

It is designed to ingest multiple tables with different formats (CSV/JSON) and predefined schema hints. Auto Loader processes new files incrementally, handles schema evolution, and guarantees reliable ingestion through checkpointing.

For each table, the module:
- Reads raw files from the source container
- Applies schema hints to enforce structure
- Writes the data into Delta tables in the bronze container
- Maintains checkpoints for idempotent, exactly-once processing
- Logs progress and errors for easier monitoring

This ensures a robust, scalable, and automated pipeline for loading raw operational data into the bronze layer, preparing it for downstream transformations in the silver and gold layers.

### 🎯 Purpose of the Job
The job prepares the Unity Catalog environment, loads raw data into the Lakehouse, and executes all transformations in the Medallion Architecture:

1. Environment Setup
    - Creates catalogs and schemas
    - Creates external locations
2. Staging (Source → Bronze)
3. Bronze transformations
4. Silver transformations
5. Gold transformations

Everything is parameterized so the same job can be reused across development, testing, and production just by changing one parameter (`catalog_name`).

#### 🧩 Key Feature: catalog_name Parameter (Environment Switching)
The job defines a top-level parameter:
```yaml
parameters:
  - name: catalog_name
    default: ap_dev
```
This allows you to run the exact same ETL flow but target different workspaces/catalogs:
- ap_dev → development
- ap_test → testing
- ap_prd → production

Each ETL python script receives this value as:
```yaml
parameters:
  - --catalog_name
  - "{{job.parameters.catalog_name}}"
```

You can configure this in the Jobs & Pipelines databricks UI 
In the job parameter write the value pair (example for dev workspace):
`catalog_name: ap_dev`

In the task parameter of each python file task you need to write:
```json
["--catalog_name","{{job.parameters.catalog_name}}"]
```

This makes the entire pipeline environment-agnostic, avoiding code duplication and enabling CI/CD-friendly deployments.

<img width="1600" height="645" alt="job_dag" src="https://github.com/user-attachments/assets/c0daba85-8178-4973-a542-dd5c0ce92457" />



## 👤 Author

**Arturo Carrillo Villarreal**
