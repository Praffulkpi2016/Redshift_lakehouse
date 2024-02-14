DROP TABLE IF EXISTS bronze_bec_ods_stg.AR_CUSTOMERS;
CREATE TABLE bronze_bec_ods_stg.AR_CUSTOMERS AS
SELECT
  *
FROM bec_raw_dl_ext.AR_CUSTOMERS
WHERE
  kca_operation <> 'DELETE';