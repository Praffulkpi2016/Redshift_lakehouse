DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_LEDGER_NORM_SEG_VALS;
CREATE TABLE bronze_bec_ods_stg.GL_LEDGER_NORM_SEG_VALS AS
SELECT
  *
FROM bec_raw_dl_ext.GL_LEDGER_NORM_SEG_VALS
WHERE
  kca_operation <> 'DELETE'
  AND (RECORD_ID, last_update_date) IN (
    SELECT
      RECORD_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_LEDGER_NORM_SEG_VALS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      RECORD_ID
  );