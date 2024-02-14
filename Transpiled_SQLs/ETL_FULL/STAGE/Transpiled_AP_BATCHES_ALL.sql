DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_BATCHES_ALL;
CREATE TABLE bronze_bec_ods_stg.AP_BATCHES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AP_BATCHES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (batch_id, last_update_date) IN (
    SELECT
      batch_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_BATCHES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      batch_id
  );