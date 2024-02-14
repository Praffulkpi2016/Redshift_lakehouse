DROP TABLE IF EXISTS bronze_bec_ods_stg.MSC_PLANS;
CREATE TABLE bronze_bec_ods_stg.MSC_PLANS AS
SELECT
  *
FROM bec_raw_dl_ext.MSC_PLANS
WHERE
  kca_operation <> 'DELETE';