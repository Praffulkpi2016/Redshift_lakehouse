DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_LEDGERS;
CREATE TABLE bronze_bec_ods_stg.GL_LEDGERS AS
SELECT
  *
FROM bec_raw_dl_ext.GL_LEDGERS
WHERE
  kca_operation <> 'DELETE'
  AND (LEDGER_ID, last_update_date) IN (
    SELECT
      LEDGER_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_LEDGERS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      LEDGER_ID
  );