/* Delete Records */
DELETE FROM silver_bec_ods.GL_LEDGER_CONFIG_DETAILS
WHERE
  (CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE) IN (
    SELECT
      stg.CONFIGURATION_ID,
      stg.OBJECT_ID,
      stg.OBJECT_TYPE_CODE,
      stg.SETUP_STEP_CODE
    FROM silver_bec_ods.GL_LEDGER_CONFIG_DETAILS AS ods, bronze_bec_ods_stg.GL_LEDGER_CONFIG_DETAILS AS stg
    WHERE
      ods.CONFIGURATION_ID = stg.CONFIGURATION_ID
      AND ods.OBJECT_ID = stg.OBJECT_ID
      AND ods.OBJECT_TYPE_CODE = stg.OBJECT_TYPE_CODE
      AND ods.SETUP_STEP_CODE = stg.SETUP_STEP_CODE
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.GL_LEDGER_CONFIG_DETAILS (
  configuration_id,
  object_type_code,
  object_id,
  object_name,
  setup_step_code,
  next_action_code,
  status_code,
  created_by,
  last_update_login,
  last_update_date,
  last_updated_by,
  creation_date,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    configuration_id,
    object_type_code,
    object_id,
    object_name,
    setup_step_code,
    next_action_code,
    status_code,
    created_by,
    last_update_login,
    last_update_date,
    last_updated_by,
    creation_date,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.GL_LEDGER_CONFIG_DETAILS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE, kca_seq_id) IN (
      SELECT
        CONFIGURATION_ID,
        OBJECT_ID,
        OBJECT_TYPE_CODE,
        SETUP_STEP_CODE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.GL_LEDGER_CONFIG_DETAILS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        CONFIGURATION_ID,
        OBJECT_ID,
        OBJECT_TYPE_CODE,
        SETUP_STEP_CODE
    )
);
/* Soft delete */
UPDATE silver_bec_ods.GL_LEDGER_CONFIG_DETAILS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.GL_LEDGER_CONFIG_DETAILS SET IS_DELETED_FLG = 'Y'
WHERE
  (CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE) IN (
    SELECT
      CONFIGURATION_ID,
      OBJECT_ID,
      OBJECT_TYPE_CODE,
      SETUP_STEP_CODE
    FROM bec_raw_dl_ext.GL_LEDGER_CONFIG_DETAILS
    WHERE
      (CONFIGURATION_ID, OBJECT_ID, OBJECT_TYPE_CODE, SETUP_STEP_CODE, KCA_SEQ_ID) IN (
        SELECT
          CONFIGURATION_ID,
          OBJECT_ID,
          OBJECT_TYPE_CODE,
          SETUP_STEP_CODE,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.GL_LEDGER_CONFIG_DETAILS
        GROUP BY
          CONFIGURATION_ID,
          OBJECT_ID,
          OBJECT_TYPE_CODE,
          SETUP_STEP_CODE
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_ledger_config_details';