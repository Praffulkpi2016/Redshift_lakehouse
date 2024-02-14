DROP TABLE IF EXISTS bronze_bec_ods_stg.AR_ADJUSTMENTS_ALL;
CREATE TABLE bronze_bec_ods_stg.AR_ADJUSTMENTS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AR_ADJUSTMENTS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (ADJUSTMENT_ID, last_update_date) IN (
    SELECT
      ADJUSTMENT_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AR_ADJUSTMENTS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      ADJUSTMENT_ID
  );