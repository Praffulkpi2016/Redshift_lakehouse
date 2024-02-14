DROP TABLE IF EXISTS bronze_bec_ods_stg.fa_adjustments;
CREATE TABLE bronze_bec_ods_stg.fa_adjustments AS
SELECT
  *
FROM bec_raw_dl_ext.fa_adjustments
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(TRANSACTION_HEADER_ID, 0), COALESCE(ADJUSTMENT_LINE_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(TRANSACTION_HEADER_ID, 0) AS TRANSACTION_HEADER_ID,
      COALESCE(ADJUSTMENT_LINE_ID, 0) AS ADJUSTMENT_LINE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.fa_adjustments
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(TRANSACTION_HEADER_ID, 0),
      COALESCE(ADJUSTMENT_LINE_ID, 0)
  );