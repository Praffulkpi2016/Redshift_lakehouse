TRUNCATE table bronze_bec_ods_stg.GL_BUDGET_VERSIONS;
INSERT INTO bronze_bec_ods_stg.GL_BUDGET_VERSIONS (
  budget_version_id,
  last_update_date,
  last_updated_by,
  budget_type,
  budget_name,
  version_num,
  status,
  date_opened,
  creation_date,
  created_by,
  last_update_login,
  description,
  date_active,
  date_archived,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  context,
  control_budget_version_id,
  igi_bud_nyc_flag,
  KCA_OPERATION,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    budget_version_id,
    last_update_date,
    last_updated_by,
    budget_type,
    budget_name,
    version_num,
    status,
    date_opened,
    creation_date,
    created_by,
    last_update_login,
    description,
    date_active,
    date_archived,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    context,
    control_budget_version_id,
    igi_bud_nyc_flag,
    KCA_OPERATION,
    KCA_SEQ_ID,
    kca_seq_date
  FROM bec_raw_dl_ext.GL_BUDGET_VERSIONS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (BUDGET_VERSION_ID, KCA_SEQ_ID) IN (
      SELECT
        BUDGET_VERSION_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.GL_BUDGET_VERSIONS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        BUDGET_VERSION_ID
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_budget_versions'
      )
    )
);