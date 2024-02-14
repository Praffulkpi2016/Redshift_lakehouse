TRUNCATE table bronze_bec_ods_stg.AR_AGING_BUCKETS;
INSERT INTO bronze_bec_ods_stg.AR_AGING_BUCKETS (
  aging_bucket_id,
  last_updated_by,
  last_update_date,
  last_update_login,
  created_by,
  creation_date,
  bucket_name,
  status,
  aging_type,
  description,
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
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    aging_bucket_id,
    last_updated_by,
    last_update_date,
    last_update_login,
    created_by,
    creation_date,
    bucket_name,
    status,
    aging_type,
    description,
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
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.AR_AGING_BUCKETS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (aging_bucket_id, kca_seq_id) IN (
      SELECT
        aging_bucket_id,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.AR_AGING_BUCKETS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        aging_bucket_id
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ar_aging_buckets'
    )
);