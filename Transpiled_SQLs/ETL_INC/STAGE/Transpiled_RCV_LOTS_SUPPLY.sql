TRUNCATE table bronze_bec_ods_stg.RCV_LOTS_SUPPLY;
INSERT INTO bronze_bec_ods_stg.RCV_LOTS_SUPPLY (
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
  kca_seq_id,
  KCA_SEQ_DATE
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
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.RCV_LOTS_SUPPLY
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(SUPPLY_TYPE_CODE, ''), COALESCE(SHIPMENT_LINE_ID, 0), COALESCE(LOT_NUM, ''), quantity, kca_seq_id) IN (
      SELECT
        COALESCE(SUPPLY_TYPE_CODE, ''),
        COALESCE(SHIPMENT_LINE_ID, 0),
        COALESCE(LOT_NUM, ''),
        quantity,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.RCV_LOTS_SUPPLY
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(SUPPLY_TYPE_CODE, ''),
        COALESCE(SHIPMENT_LINE_ID, 0),
        COALESCE(LOT_NUM, ''),
        quantity
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'rcv_lots_supply'
      )
    )
);