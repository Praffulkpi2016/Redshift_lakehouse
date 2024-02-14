DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_LEDGER_CONFIGURATIONS;
CREATE TABLE bronze_bec_ods_stg.GL_LEDGER_CONFIGURATIONS AS
SELECT
  *
FROM bec_raw_dl_ext.GL_LEDGER_CONFIGURATIONS
WHERE
  kca_operation <> 'DELETE'
  AND (CONFIGURATION_ID, last_update_date) IN (
    SELECT
      CONFIGURATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_LEDGER_CONFIGURATIONS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      CONFIGURATION_ID
  );