DROP TABLE IF EXISTS silver_bec_ods.RCV_ROUTING_HEADERS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.RCV_ROUTING_HEADERS (
  routing_header_id DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_updated_login DECIMAL(15, 0),
  routing_name STRING,
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
  description STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.RCV_ROUTING_HEADERS (
  routing_header_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_updated_login,
  routing_name,
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
  description,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  routing_header_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_updated_login,
  routing_name,
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
  description,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.RCV_ROUTING_HEADERS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'rcv_routing_headers';