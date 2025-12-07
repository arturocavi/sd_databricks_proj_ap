-- ==========================================================
-- Unity Catalog Governance for AP
-- Roles: Data Engineers (Editors), Analytics Team (Readers)
-- ==========================================================

-- -----------------------------------------------------------
-- Grant Catalog-Level Access (minimum required)
-- -----------------------------------------------------------
-- Data engineers need full write access to the schema
GRANT USE CATALOG ON CATALOG ap TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA ap.gold TO `data_engineers`;

-- Analytics readers need only read
GRANT USE CATALOG ON CATALOG ap TO `analytics_team`;
GRANT USE SCHEMA ON SCHEMA ap.gold TO `analytics_team`;

-- -----------------------------------------------------------
-- Schema-Level Permissions
-- -----------------------------------------------------------

-- ============================
-- DATA ENGINEERS (Editors)
-- ============================
-- Best practice: Assign full editing via group
GRANT SELECT ON SCHEMA ap.gold TO `data_engineers`;
GRANT MODIFY ON SCHEMA ap.gold TO `data_engineers`;
GRANT CREATE TABLE ON SCHEMA ap.gold TO `data_engineers`;
GRANT CREATE FUNCTION ON SCHEMA ap.gold TO `data_engineers`;
GRANT CREATE MODEL ON SCHEMA ap.gold TO `data_engineers`;
GRANT CREATE MATERIALIZED VIEW ON SCHEMA ap.gold TO `data_engineers`;
GRANT CREATE VOLUME ON SCHEMA ap.gold TO `data_engineers`;
GRANT WRITE VOLUME ON SCHEMA ap.gold TO `data_engineers`;
GRANT APPLY TAG ON SCHEMA ap.gold TO `data_engineers`;

-- ============================
-- ANALYTICS TEAM (Read-only)
-- ============================
GRANT SELECT ON SCHEMA ap.gold TO `analytics_team`;
GRANT READ VOLUME ON SCHEMA ap.gold TO `analytics_team`;

-- No modify, no create:
-- Explicitly *not* granted MODIFY, CREATE TABLE, etc.

-- ============================
-- SPECIAL USER: Ever (Elevated Permissions)
-- ============================
GRANT MODIFY ON SCHEMA ap.gold TO `ever@smartdatacorp.com`;
GRANT CREATE TABLE ON SCHEMA ap.gold TO `ever@smartdatacorp.com`;

-- Allow Ever to manage grants
GRANT ALL PRIVILEGES ON SCHEMA ap.gold TO `ever@smartdatacorp.com`;

-- -----------------------------------------------------------
-- Table-level permissions
-- -----------------------------------------------------------

-- Limit analytics team to 2 silver table only
GRANT SELECT ON TABLE ap.silver.ap_invoices TO `analytics_team`;
GRANT SELECT ON TABLE ap.silver.suppliers TO `analytics_team`;
