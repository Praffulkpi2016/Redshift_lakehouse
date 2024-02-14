/* Delete Records */
DELETE FROM silver_bec_ods.RCV_LOTS_SUPPLY
WHERE
  (COALESCE(SUPPLY_TYPE_CODE, ''), COALESCE(SHIPMENT_LINE_ID, 0), COALESCE(LOT_NUM, ''), quantity) IN (
    SELECT
      COALESCE(stg.SUPPLY_TYPE_CODE, ''),
      COALESCE(stg.SHIPMENT_LINE_ID, 0),
      COALESCE(stg.LOT_NUM, ''),
      stg.quantity
    FROM silver_bec_ods.RCV_LOTS_SUPPLY AS ods, bronze_bec_ods_stg.RCV_LOTS_SUPPLY AS stg
    WHERE
      ods.SUPPLY_TYPE_CODE = stg.SUPPLY_TYPE_CODE
      AND ods.SHIPMENT_LINE_ID = stg.SHIPMENT_LINE_ID
      AND ods.LOT_NUM = stg.LOT_NUM
      AND ods.quantity = stg.quantity
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.RCV_LOTS_SUPPLY (
  supply_type_code,
  lot_num,
  quantity,
  primary_quantity,
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
  expiration_date,
  secondary_quantity,
  sublot_num,
  reason_code,
  reason_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    supply_type_code,
    lot_num,
    quantity,
    primary_quantity,
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
    expiration_date,
    secondary_quantity,
    sublot_num,
    reason_code,
    reason_id,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.RCV_LOTS_SUPPLY
  WHERE
    kca_operation IN ('INSERT')
    AND (COALESCE(SUPPLY_TYPE_CODE, ''), COALESCE(SHIPMENT_LINE_ID, 0), COALESCE(LOT_NUM, ''), quantity, kca_seq_id) IN (
      SELECT
        COALESCE(SUPPLY_TYPE_CODE, ''),
        COALESCE(SHIPMENT_LINE_ID, 0),
        COALESCE(LOT_NUM, ''),
        quantity,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.RCV_LOTS_SUPPLY
      WHERE
        kca_operation IN ('INSERT')
      GROUP BY
        COALESCE(SUPPLY_TYPE_CODE, ''),
        COALESCE(SHIPMENT_LINE_ID, 0),
        COALESCE(LOT_NUM, ''),
        quantity
    )
);
/* Soft delete */
UPDATE silver_bec_ods.RCV_LOTS_SUPPLY SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.RCV_LOTS_SUPPLY SET IS_DELETED_FLG = 'Y'
WHERE
  (SUPPLY_TYPE_CODE, SHIPMENT_LINE_ID, LOT_NUM, quantity) IN (
    SELECT
      SUPPLY_TYPE_CODE,
      SHIPMENT_LINE_ID,
      LOT_NUM,
      quantity
    FROM bec_raw_dl_ext.RCV_LOTS_SUPPLY
    WHERE
      (SUPPLY_TYPE_CODE, SHIPMENT_LINE_ID, LOT_NUM, quantity, KCA_SEQ_ID) IN (
        SELECT
          SUPPLY_TYPE_CODE,
          SHIPMENT_LINE_ID,
          LOT_NUM,
          quantity,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.RCV_LOTS_SUPPLY
        GROUP BY
          SUPPLY_TYPE_CODE,
          SHIPMENT_LINE_ID,
          LOT_NUM,
          quantity
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'rcv_lots_supply';