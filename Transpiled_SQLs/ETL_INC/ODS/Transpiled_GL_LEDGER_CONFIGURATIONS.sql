/* Delete Records */
DELETE FROM silver_bec_ods.GL_LEDGER_CONFIGURATIONS
WHERE
  (
    CONFIGURATION_ID
  ) IN (
    SELECT
      stg.CONFIGURATION_ID
    FROM silver_bec_ods.GL_LEDGER_CONFIGURATIONS AS ods, bronze_bec_ods_stg.GL_LEDGER_CONFIGURATIONS AS stg
    WHERE
      ods.CONFIGURATION_ID = stg.CONFIGURATION_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.GL_LEDGER_CONFIGURATIONS (
  configuration_id,
  `name`,
  description,
  completion_status_code,
  acctg_environment_code,
  creation_date,
  created_by,
  last_update_login,
  last_update_date,
  last_updated_by,
  context,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    configuration_id,
    `name`,
    description,
    completion_status_code,
    acctg_environment_code,
    creation_date,
    created_by,
    last_update_login,
    last_update_date,
    last_updated_by,
    context,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.GL_LEDGER_CONFIGURATIONS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (CONFIGURATION_ID, kca_seq_id) IN (
      SELECT
        CONFIGURATION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.GL_LEDGER_CONFIGURATIONS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        CONFIGURATION_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.GL_LEDGER_CONFIGURATIONS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.GL_LEDGER_CONFIGURATIONS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    CONFIGURATION_ID
  ) IN (
    SELECT
      CONFIGURATION_ID
    FROM bec_raw_dl_ext.GL_LEDGER_CONFIGURATIONS
    WHERE
      (CONFIGURATION_ID, KCA_SEQ_ID) IN (
        SELECT
          CONFIGURATION_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.GL_LEDGER_CONFIGURATIONS
        GROUP BY
          CONFIGURATION_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_ledger_configurations';