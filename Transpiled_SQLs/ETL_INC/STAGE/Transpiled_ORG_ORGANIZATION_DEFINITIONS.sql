DROP TABLE IF EXISTS bronze_bec_ods_stg.ORG_ORGANIZATION_DEFINITIONS;
CREATE TABLE bronze_bec_ods_stg.ORG_ORGANIZATION_DEFINITIONS AS
SELECT
  *
FROM bec_raw_dl_ext.ORG_ORGANIZATION_DEFINITIONS
WHERE
  kca_operation <> 'DELETE';