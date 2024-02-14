DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_LEDGER_CONFIG_DETAILS;
CREATE TABLE bronze_bec_ods_stg.GL_LEDGER_CONFIG_DETAILS AS
SELECT
  *
FROM bec_raw_dl_ext.GL_LEDGER_CONFIG_DETAILS
WHERE
  kca_operation <> 'DELETE'
  AND (CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE, last_update_date) IN (
    SELECT
      CONFIGURATION_ID,
      OBJECT_ID,
      OBJECT_TYPE_CODE,
      SETUP_STEP_CODE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_LEDGER_CONFIG_DETAILS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      CONFIGURATION_ID,
      OBJECT_ID,
      OBJECT_TYPE_CODE,
      SETUP_STEP_CODE
  );