TRUNCATE table bronze_bec_ods_stg.RCV_SERIALS_SUPPLY;
INSERT INTO bronze_bec_ods_stg.RCV_SERIALS_SUPPLY (
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
  kca_operation,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
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
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.RCV_SERIALS_SUPPLY
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (SHIPMENT_LINE_ID, SERIAL_NUM, kca_seq_id) IN (
      SELECT
        SHIPMENT_LINE_ID,
        SERIAL_NUM,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.RCV_SERIALS_SUPPLY
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        SHIPMENT_LINE_ID,
        SERIAL_NUM
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'rcv_serials_supply'
      )
    )
);