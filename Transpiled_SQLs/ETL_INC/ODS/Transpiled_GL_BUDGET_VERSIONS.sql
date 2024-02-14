/* Delete Records */
DELETE FROM silver_bec_ods.GL_BUDGET_VERSIONS
WHERE
  (
    BUDGET_VERSION_ID
  ) IN (
    SELECT
      stg.BUDGET_VERSION_ID
    FROM silver_bec_ods.GL_BUDGET_VERSIONS AS ods, bronze_bec_ods_stg.GL_BUDGET_VERSIONS AS stg
    WHERE
      ods.BUDGET_VERSION_ID = stg.BUDGET_VERSION_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.GL_BUDGET_VERSIONS (
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
  IS_DELETED_FLG,
  kca_seq_id,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.GL_BUDGET_VERSIONS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (BUDGET_VERSION_ID, KCA_SEQ_ID) IN (
      SELECT
        BUDGET_VERSION_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.GL_BUDGET_VERSIONS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        BUDGET_VERSION_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.GL_BUDGET_VERSIONS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.GL_BUDGET_VERSIONS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    BUDGET_VERSION_ID
  ) IN (
    SELECT
      BUDGET_VERSION_ID
    FROM bec_raw_dl_ext.GL_BUDGET_VERSIONS
    WHERE
      (BUDGET_VERSION_ID, KCA_SEQ_ID) IN (
        SELECT
          BUDGET_VERSION_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.GL_BUDGET_VERSIONS
        GROUP BY
          BUDGET_VERSION_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_budget_versions';