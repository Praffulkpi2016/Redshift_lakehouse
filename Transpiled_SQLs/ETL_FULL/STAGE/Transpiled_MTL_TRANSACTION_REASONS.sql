DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_TRANSACTION_REASONS;
CREATE TABLE bronze_bec_ods_stg.MTL_TRANSACTION_REASONS AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_TRANSACTION_REASONS
WHERE
  kca_operation <> 'DELETE'
  AND (REASON_ID, last_update_date) IN (
    SELECT
      REASON_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_TRANSACTION_REASONS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      REASON_ID
  );