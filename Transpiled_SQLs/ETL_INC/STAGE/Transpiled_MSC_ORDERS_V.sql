DROP TABLE IF EXISTS bronze_bec_ods_stg.MSC_ORDERS_V;
CREATE TABLE bronze_bec_ods_stg.MSC_ORDERS_V AS
SELECT
  *
FROM bec_raw_dl_ext.MSC_ORDERS_V;