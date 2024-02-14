TRUNCATE table bronze_bec_ods_stg.AR_COLLECTORS;
INSERT INTO bronze_bec_ods_stg.AR_COLLECTORS (
  collector_id,
  last_updated_by,
  last_update_date,
  last_update_login,
  creation_date,
  created_by,
  `name`,
  employee_id,
  description,
  status,
  inactive_date,
  alias,
  telephone_number,
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
  resource_id,
  resource_type,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    collector_id,
    last_updated_by,
    last_update_date,
    last_update_login,
    creation_date,
    created_by,
    `name`,
    employee_id,
    description,
    status,
    inactive_date,
    alias,
    telephone_number,
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
    resource_id,
    resource_type,
    zd_edition_name,
    zd_sync,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.AR_COLLECTORS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COLLECTOR_ID, kca_seq_id) IN (
      SELECT
        COLLECTOR_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.AR_COLLECTORS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COLLECTOR_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ar_collectors'
    )
);