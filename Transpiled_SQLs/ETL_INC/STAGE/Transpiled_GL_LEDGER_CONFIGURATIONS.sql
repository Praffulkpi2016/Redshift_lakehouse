TRUNCATE table bronze_bec_ods_stg.GL_LEDGER_CONFIGURATIONS;
INSERT INTO bronze_bec_ods_stg.GL_LEDGER_CONFIGURATIONS (
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
  KCA_OPERATION,
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
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.GL_LEDGER_CONFIGURATIONS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (CONFIGURATION_ID, kca_seq_id) IN (
      SELECT
        CONFIGURATION_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.GL_LEDGER_CONFIGURATIONS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        CONFIGURATION_ID
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_ledger_configurations'
      )
    )
);