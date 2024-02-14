TRUNCATE table bronze_bec_ods_stg.cst_activities;
INSERT INTO bronze_bec_ods_stg.cst_activities (
  activity_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  activity,
  organization_id,
  description,
  default_basis_type,
  disable_date,
  output_uom,
  value_added_activity_flag,
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
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    activity_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    activity,
    organization_id,
    description,
    default_basis_type,
    disable_date,
    output_uom,
    value_added_activity_flag,
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
    zd_edition_name,
    zd_sync,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.cst_activities
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (ACTIVITY_ID, kca_seq_id) IN (
      SELECT
        ACTIVITY_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.cst_activities
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        ACTIVITY_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'cst_activities'
    )
);