/* Delete Records */
DELETE FROM silver_bec_ods.MTL_GENERIC_DISPOSITIONS
WHERE
  (COALESCE(DISPOSITION_ID, 0), COALESCE(ORGANIZATION_ID, 0)) IN (
    SELECT
      COALESCE(stg.DISPOSITION_ID, 0) AS DISPOSITION_ID,
      COALESCE(stg.ORGANIZATION_ID, 0) AS ORGANIZATION_ID
    FROM silver_bec_ods.MTL_GENERIC_DISPOSITIONS AS ods, bronze_bec_ods_stg.MTL_GENERIC_DISPOSITIONS AS stg
    WHERE
      COALESCE(ods.DISPOSITION_ID, 0) = COALESCE(stg.DISPOSITION_ID, 0)
      AND COALESCE(ods.ORGANIZATION_ID, 0) = COALESCE(stg.ORGANIZATION_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_GENERIC_DISPOSITIONS (
  disposition_id,
  organization_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  description,
  disable_date,
  effective_date,
  distribution_account,
  segment1,
  segment2,
  segment3,
  segment4,
  segment5,
  segment6,
  segment7,
  segment8,
  segment9,
  segment10,
  segment11,
  segment12,
  segment13,
  segment14,
  segment15,
  segment16,
  segment17,
  segment18,
  segment19,
  segment20,
  summary_flag,
  enabled_flag,
  start_date_active,
  end_date_active,
  attribute_category,
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
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    disposition_id,
    organization_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    description,
    disable_date,
    effective_date,
    distribution_account,
    segment1,
    segment2,
    segment3,
    segment4,
    segment5,
    segment6,
    segment7,
    segment8,
    segment9,
    segment10,
    segment11,
    segment12,
    segment13,
    segment14,
    segment15,
    segment16,
    segment17,
    segment18,
    segment19,
    segment20,
    summary_flag,
    enabled_flag,
    start_date_active,
    end_date_active,
    attribute_category,
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
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_GENERIC_DISPOSITIONS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(DISPOSITION_ID, 0), COALESCE(ORGANIZATION_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(DISPOSITION_ID, 0) AS DISPOSITION_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MTL_GENERIC_DISPOSITIONS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(DISPOSITION_ID, 0),
        COALESCE(ORGANIZATION_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_GENERIC_DISPOSITIONS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_GENERIC_DISPOSITIONS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(DISPOSITION_ID, 0), COALESCE(ORGANIZATION_ID, 0)) IN (
    SELECT
      COALESCE(DISPOSITION_ID, 0),
      COALESCE(ORGANIZATION_ID, 0)
    FROM bec_raw_dl_ext.MTL_GENERIC_DISPOSITIONS
    WHERE
      (COALESCE(DISPOSITION_ID, 0), COALESCE(ORGANIZATION_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(DISPOSITION_ID, 0),
          COALESCE(ORGANIZATION_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_GENERIC_DISPOSITIONS
        GROUP BY
          COALESCE(DISPOSITION_ID, 0),
          COALESCE(ORGANIZATION_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_generic_dispositions';