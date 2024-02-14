TRUNCATE table bronze_bec_ods_stg.PO_ASL_STATUSES;
INSERT INTO bronze_bec_ods_stg.PO_ASL_STATUSES (
  status_id,
  status,
  status_description,
  asl_default_flag,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  inactive_date,
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
  last_update_login,
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
    status_id,
    status,
    status_description,
    asl_default_flag,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    inactive_date,
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
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    zd_edition_name,
    zd_sync,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.PO_ASL_STATUSES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(STATUS_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(STATUS_ID, 0) AS STATUS_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.PO_ASL_STATUSES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(STATUS_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'po_asl_statuses'
    )
);