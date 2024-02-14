DROP TABLE IF EXISTS silver_bec_ods.PO_LINE_TYPES_VL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.PO_LINE_TYPES_VL (
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
  outside_operation_flag STRING,
  request_id DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  receive_close_tolerance DECIMAL(15, 0),
  line_type_id DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  order_type_lookup_code STRING,
  last_update_login DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  category_id DECIMAL(15, 0),
  unit_of_measure STRING,
  unit_price DECIMAL(28, 10),
  receiving_flag STRING,
  inactive_date TIMESTAMP,
  attribute_category STRING,
  attribute1 STRING,
  attribute2 STRING,
  line_type STRING,
  description STRING,
  purchase_basis STRING,
  matching_basis STRING,
  CLM_SEVERABLE_FLAG STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.PO_LINE_TYPES_VL (
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
  outside_operation_flag,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  receive_close_tolerance,
  line_type_id,
  last_update_date,
  last_updated_by,
  order_type_lookup_code,
  last_update_login,
  creation_date,
  created_by,
  category_id,
  unit_of_measure,
  unit_price,
  receiving_flag,
  inactive_date,
  attribute_category,
  attribute1,
  attribute2,
  line_type,
  description,
  purchase_basis,
  matching_basis,
  CLM_SEVERABLE_FLAG,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
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
  outside_operation_flag,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  receive_close_tolerance,
  line_type_id,
  last_update_date,
  last_updated_by,
  order_type_lookup_code,
  last_update_login,
  creation_date,
  created_by,
  category_id,
  unit_of_measure,
  unit_price,
  receiving_flag,
  inactive_date,
  attribute_category,
  attribute1,
  attribute2,
  line_type,
  description,
  purchase_basis,
  matching_basis,
  CLM_SEVERABLE_FLAG,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.PO_LINE_TYPES_VL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_line_types_vl';