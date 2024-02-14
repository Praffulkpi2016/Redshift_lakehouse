DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_VENDORS;
CREATE TABLE bronze_bec_ods_stg.PO_VENDORS AS
SELECT
  *
FROM bec_raw_dl_ext.PO_VENDORS
WHERE
  kca_operation <> 'DELETE';