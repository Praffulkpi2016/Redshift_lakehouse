DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_LEDGER_SEGMENT_VALUES;
CREATE TABLE bronze_bec_ods_stg.GL_LEDGER_SEGMENT_VALUES AS
SELECT
  *
FROM bec_raw_dl_ext.GL_LEDGER_SEGMENT_VALUES
WHERE
  kca_operation <> 'DELETE'
  AND (LEDGER_ID, SEGMENT_TYPE_CODE, SEGMENT_VALUE, last_update_date) IN (
    SELECT
      LEDGER_ID,
      SEGMENT_TYPE_CODE,
      SEGMENT_VALUE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_LEDGER_SEGMENT_VALUES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      LEDGER_ID,
      SEGMENT_TYPE_CODE,
      SEGMENT_VALUE
  );