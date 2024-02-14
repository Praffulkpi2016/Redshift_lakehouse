DROP TABLE IF EXISTS silver_bec_ods.RCV_SERIALS_SUPPLY;
CREATE TABLE IF NOT EXISTS silver_bec_ods.RCV_SERIALS_SUPPLY (
  supply_type_code STRING,
  serial_num STRING,
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  request_id DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  shipment_line_id DECIMAL(15, 0),
  transaction_id DECIMAL(15, 0),
  lot_num STRING,
  vendor_serial_num STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.RCV_SERIALS_SUPPLY (
  supply_type_code,
  serial_num,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  shipment_line_id,
  transaction_id,
  lot_num,
  vendor_serial_num,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  supply_type_code,
  serial_num,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  shipment_line_id,
  transaction_id,
  lot_num,
  vendor_serial_num,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.RCV_SERIALS_SUPPLY;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'rcv_serials_supply';