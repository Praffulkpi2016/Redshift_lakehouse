DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_JE_BATCHES;
CREATE TABLE bronze_bec_ods_stg.GL_JE_BATCHES AS
SELECT
  *
FROM bec_raw_dl_ext.GL_JE_BATCHES
WHERE
  kca_operation <> 'DELETE'
  AND (je_batch_id, last_update_date) IN (
    SELECT
      je_batch_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_JE_BATCHES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      je_batch_id
  );