DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_PERIOD_STATUSES;
CREATE TABLE bronze_bec_ods_stg.GL_PERIOD_STATUSES AS
SELECT
  *
FROM bec_raw_dl_ext.GL_PERIOD_STATUSES
WHERE
  kca_operation <> 'DELETE'
  AND (APPLICATION_ID, LEDGER_ID, PERIOD_NAME, last_update_date) IN (
    SELECT
      APPLICATION_ID,
      LEDGER_ID,
      PERIOD_NAME,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_PERIOD_STATUSES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      APPLICATION_ID,
      LEDGER_ID,
      PERIOD_NAME
  );