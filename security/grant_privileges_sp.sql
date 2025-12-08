-- ==========================================================
-- Grant Privileges to Service Principals (SP)
-- SP that will run the job: arturocvsdpf@outlook.com
-- ==========================================================

-- -----------------------------------------------------------
-- Grant CREATE Privileges
-- -----------------------------------------------------------
-- Give the SP permission to create catalogs
GRANT CREATE CATALOG ON METASTORE TO `arturocvsdpf@outlook.com`;

-- Give the SP permission to create external locations
GRANT CREATE EXTERNAL LOCATION ON METASTORE TO `arturocvsdpf@outlook.com`;

-- Allow SP to create storage credentials (optional)
GRANT CREATE STORAGE CREDENTIAL ON METASTORE TO `arturocvsdpf@outlook.com`;



-- -----------------------------------------------------------
-- Grant CREATE EXTERNAL LOCATION on Specific Storage Credential
-- -----------------------------------------------------------
-- Grant CREATE EXTERNAL LOCATION on Credential 'sd0212_credential'
GRANT CREATE EXTERNAL LOCATION
ON STORAGE CREDENTIAL `sd0212_credential`
TO `arturocvsdpf@outlook.com`;



-- -----------------------------------------------------------
-- Grant READ FILES on Specific External Location
-- -----------------------------------------------------------
-- Grant READ FILES on External Location 'bronze_ext_loc'
GRANT READ FILES
ON EXTERNAL LOCATION `bronze_ext_loc`
TO `arturocvsdpf@outlook.com`;

