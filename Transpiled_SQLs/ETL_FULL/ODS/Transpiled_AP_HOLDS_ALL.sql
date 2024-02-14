DROP table IF EXISTS silver_bec_ods.ap_holds_all;
CREATE TABLE IF NOT EXISTS silver_bec_ods.ap_holds_all (
  invoice_id DECIMAL(15, 0),
  line_location_id DECIMAL(15, 0),
  hold_lookup_code STRING,
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  held_by DECIMAL(15, 0),
  hold_date TIMESTAMP,
  hold_reason STRING,
  release_lookup_code STRING,
  release_reason STRING,
  status_flag STRING,
  last_update_login DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
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
  org_id DECIMAL(15, 0),
  responsibility_id DECIMAL(15, 0),
  rcv_transaction_id DECIMAL(15, 0),
  hold_details STRING,
  line_number DECIMAL(15, 0),
  hold_id DECIMAL(15, 0),
  wf_status STRING,
  VALIDATION_REQUEST_ID DECIMAL(15, 0),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.ap_holds_all (
  invoice_id,
  line_location_id,
  hold_lookup_code,
  last_update_date,
  last_updated_by,
  held_by,
  hold_date,
  hold_reason,
  release_lookup_code,
  release_reason,
  status_flag,
  last_update_login,
  creation_date,
  created_by,
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
  org_id,
  responsibility_id,
  rcv_transaction_id,
  hold_details,
  line_number,
  hold_id,
  wf_status,
  validation_request_id,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  invoice_id,
  line_location_id,
  hold_lookup_code,
  last_update_date,
  last_updated_by,
  held_by,
  hold_date,
  hold_reason,
  release_lookup_code,
  release_reason,
  status_flag,
  last_update_login,
  creation_date,
  created_by,
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
  org_id,
  responsibility_id,
  rcv_transaction_id,
  hold_details,
  line_number,
  hold_id,
  wf_status,
  VALIDATION_REQUEST_ID,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.ap_holds_all;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_holds_all';