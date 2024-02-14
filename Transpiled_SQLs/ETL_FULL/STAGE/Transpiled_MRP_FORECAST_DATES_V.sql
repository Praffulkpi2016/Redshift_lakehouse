DROP TABLE IF EXISTS bronze_bec_ods_stg.MRP_FORECAST_DATES_V;
CREATE TABLE bronze_bec_ods_stg.MRP_FORECAST_DATES_V AS
SELECT
  *
FROM bec_raw_dl_ext.MRP_FORECAST_DATES_V
WHERE
  kca_operation <> 'DELETE';