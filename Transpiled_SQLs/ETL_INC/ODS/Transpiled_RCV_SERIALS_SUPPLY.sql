/* Delete Records */
DELETE FROM silver_bec_ods.RCV_SERIALS_SUPPLY
WHERE
  (SHIPMENT_LINE_ID, SERIAL_NUM) IN (
    SELECT
      stg.SHIPMENT_LINE_ID,
      stg.SERIAL_NUM
    FROM silver_bec_ods.RCV_SERIALS_SUPPLY AS ods, bronze_bec_ods_stg.RCV_SERIALS_SUPPLY AS stg
    WHERE
      ods.SHIPMENT_LINE_ID = stg.SHIPMENT_LINE_ID
      AND ods.SERIAL_NUM = stg.SERIAL_NUM
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
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
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
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
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.RCV_SERIALS_SUPPLY
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (SHIPMENT_LINE_ID, SERIAL_NUM, kca_seq_id) IN (
      SELECT
        SHIPMENT_LINE_ID,
        SERIAL_NUM,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.RCV_SERIALS_SUPPLY
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        SHIPMENT_LINE_ID,
        SERIAL_NUM
    )
);
/* Soft delete */
UPDATE silver_bec_ods.RCV_SERIALS_SUPPLY SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.RCV_SERIALS_SUPPLY SET IS_DELETED_FLG = 'Y'
WHERE
  (SHIPMENT_LINE_ID, SERIAL_NUM) IN (
    SELECT
      SHIPMENT_LINE_ID,
      SERIAL_NUM
    FROM bec_raw_dl_ext.RCV_SERIALS_SUPPLY
    WHERE
      (SHIPMENT_LINE_ID, SERIAL_NUM, KCA_SEQ_ID) IN (
        SELECT
          SHIPMENT_LINE_ID,
          SERIAL_NUM,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.RCV_SERIALS_SUPPLY
        GROUP BY
          SHIPMENT_LINE_ID,
          SERIAL_NUM
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'rcv_serials_supply';