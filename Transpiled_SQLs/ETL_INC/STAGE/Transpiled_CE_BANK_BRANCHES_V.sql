DROP TABLE IF EXISTS bronze_bec_ods_stg.CE_BANK_BRANCHES_V;
CREATE TABLE bronze_bec_ods_stg.CE_BANK_BRANCHES_V AS
SELECT
  *
FROM bec_raw_dl_ext.CE_BANK_BRANCHES_V
WHERE
  kca_operation <> 'DELETE';