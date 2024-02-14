DROP TABLE IF EXISTS silver_bec_ods.AR_AGING_BUCKETS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.AR_AGING_BUCKETS (
  aging_bucket_id DECIMAL(15, 0),
  last_updated_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_update_login DECIMAL(15, 0),
  created_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  bucket_name STRING,
  status STRING,
  aging_type STRING,
  description STRING,
  attribute_category STRING,
  attribute1 STRING,
  attribute2 STRING,
  attribute3 STRING,
  attribute4 STRING,
  attribute5 STRING,
  attribute6 STRING,
  attribute7 STRING,
  attribute8 STRING,
  attribute9 STRING,
  attribute10 STRING,
  attribute11 STRING,
  attribute12 STRING,
  attribute13 STRING,
  attribute14 STRING,
  attribute15 STRING,
  ZD_EDITION_NAME STRING,
  ZD_SYNC STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.AR_AGING_BUCKETS (
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
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
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
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.AR_AGING_BUCKETS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ar_aging_buckets';