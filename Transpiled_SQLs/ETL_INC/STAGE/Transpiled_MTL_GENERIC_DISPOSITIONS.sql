TRUNCATE table bronze_bec_ods_stg.MTL_GENERIC_DISPOSITIONS;
INSERT INTO bronze_bec_ods_stg.MTL_GENERIC_DISPOSITIONS (
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
    kca_operation, /* ZD_EDITION_NAME, */ /* ZD_SYNC, */
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_GENERIC_DISPOSITIONS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (DISPOSITION_ID, ORGANIZATION_ID, kca_seq_id) IN (
      SELECT
        DISPOSITION_ID,
        ORGANIZATION_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MTL_GENERIC_DISPOSITIONS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        DISPOSITION_ID,
        ORGANIZATION_ID
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_generic_dispositions'
      )
    )
);