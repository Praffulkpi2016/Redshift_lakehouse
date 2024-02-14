DROP TABLE IF EXISTS bronze_bec_ods_stg.MRP_FORECAST_DATES;
CREATE TABLE bronze_bec_ods_stg.MRP_FORECAST_DATES AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.MRP_FORECAST_DATES
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(TRANSACTION_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(TRANSACTION_ID, 0) AS TRANSACTION_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.MRP_FORECAST_DATES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(TRANSACTION_ID, 0)
    )
);