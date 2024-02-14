TRUNCATE table bronze_bec_ods_stg.mtl_txn_request_lines;
INSERT INTO bronze_bec_ods_stg.mtl_txn_request_lines (
  LINE_ID,
  HEADER_ID,
  LINE_NUMBER,
  ORGANIZATION_ID,
  INVENTORY_ITEM_ID,
  REVISION,
  FROM_SUBINVENTORY_CODE,
  FROM_LOCATOR_ID,
  TO_SUBINVENTORY_CODE,
  TO_LOCATOR_ID,
  TO_ACCOUNT_ID,
  LOT_NUMBER,
  SERIAL_NUMBER_START,
  SERIAL_NUMBER_END,
  UOM_CODE,
  QUANTITY,
  QUANTITY_DELIVERED,
  QUANTITY_DETAILED,
  DATE_REQUIRED,
  REASON_ID,
  REFERENCE,
  REFERENCE_TYPE_CODE,
  REFERENCE_ID,
  PROJECT_ID,
  TASK_ID,
  TRANSACTION_HEADER_ID,
  LINE_STATUS,
  STATUS_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  LAST_UPDATE_DATE,
  CREATED_BY,
  CREATION_DATE,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  ATTRIBUTE11,
  ATTRIBUTE12,
  ATTRIBUTE13,
  ATTRIBUTE14,
  ATTRIBUTE15,
  ATTRIBUTE_CATEGORY,
  TXN_SOURCE_ID,
  TXN_SOURCE_LINE_ID,
  TXN_SOURCE_LINE_DETAIL_ID,
  TRANSACTION_TYPE_ID,
  TRANSACTION_SOURCE_TYPE_ID,
  PRIMARY_QUANTITY,
  TO_ORGANIZATION_ID,
  PUT_AWAY_STRATEGY_ID,
  PICK_STRATEGY_ID,
  SHIP_TO_LOCATION_ID,
  UNIT_NUMBER,
  REFERENCE_DETAIL_ID,
  ASSIGNMENT_ID,
  FROM_COST_GROUP_ID,
  TO_COST_GROUP_ID,
  LPN_ID,
  TO_LPN_ID,
  PICK_SLIP_NUMBER,
  PICK_SLIP_DATE,
  FROM_SUBINVENTORY_ID,
  TO_SUBINVENTORY_ID,
  INSPECTION_STATUS,
  PICK_METHODOLOGY_ID,
  CONTAINER_ITEM_ID,
  CARTON_GROUPING_ID,
  BACKORDER_DELIVERY_DETAIL_ID,
  WMS_PROCESS_FLAG,
  SHIP_SET_ID,
  SHIP_MODEL_ID,
  MODEL_QUANTITY,
  CROSSDOCK_TYPE,
  REQUIRED_QUANTITY,
  GRADE_CODE,
  SECONDARY_QUANTITY,
  SECONDARY_QUANTITY_DELIVERED,
  SECONDARY_QUANTITY_DETAILED,
  SECONDARY_REQUIRED_QUANTITY,
  SECONDARY_UOM_CODE,
  WIP_ENTITY_ID,
  REPETITIVE_SCHEDULE_ID,
  OPERATION_SEQ_NUM,
  WIP_SUPPLY_TYPE,
  MCC_CODE,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    LINE_ID,
    HEADER_ID,
    LINE_NUMBER,
    ORGANIZATION_ID,
    INVENTORY_ITEM_ID,
    REVISION,
    FROM_SUBINVENTORY_CODE,
    FROM_LOCATOR_ID,
    TO_SUBINVENTORY_CODE,
    TO_LOCATOR_ID,
    TO_ACCOUNT_ID,
    LOT_NUMBER,
    SERIAL_NUMBER_START,
    SERIAL_NUMBER_END,
    UOM_CODE,
    QUANTITY,
    QUANTITY_DELIVERED,
    QUANTITY_DETAILED,
    DATE_REQUIRED,
    REASON_ID,
    REFERENCE,
    REFERENCE_TYPE_CODE,
    REFERENCE_ID,
    PROJECT_ID,
    TASK_ID,
    TRANSACTION_HEADER_ID,
    LINE_STATUS,
    STATUS_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    LAST_UPDATE_DATE,
    CREATED_BY,
    CREATION_DATE,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    ATTRIBUTE6,
    ATTRIBUTE7,
    ATTRIBUTE8,
    ATTRIBUTE9,
    ATTRIBUTE10,
    ATTRIBUTE11,
    ATTRIBUTE12,
    ATTRIBUTE13,
    ATTRIBUTE14,
    ATTRIBUTE15,
    ATTRIBUTE_CATEGORY,
    TXN_SOURCE_ID,
    TXN_SOURCE_LINE_ID,
    TXN_SOURCE_LINE_DETAIL_ID,
    TRANSACTION_TYPE_ID,
    TRANSACTION_SOURCE_TYPE_ID,
    PRIMARY_QUANTITY,
    TO_ORGANIZATION_ID,
    PUT_AWAY_STRATEGY_ID,
    PICK_STRATEGY_ID,
    SHIP_TO_LOCATION_ID,
    UNIT_NUMBER,
    REFERENCE_DETAIL_ID,
    ASSIGNMENT_ID,
    FROM_COST_GROUP_ID,
    TO_COST_GROUP_ID,
    LPN_ID,
    TO_LPN_ID,
    PICK_SLIP_NUMBER,
    PICK_SLIP_DATE,
    FROM_SUBINVENTORY_ID,
    TO_SUBINVENTORY_ID,
    INSPECTION_STATUS,
    PICK_METHODOLOGY_ID,
    CONTAINER_ITEM_ID,
    CARTON_GROUPING_ID,
    BACKORDER_DELIVERY_DETAIL_ID,
    WMS_PROCESS_FLAG,
    SHIP_SET_ID,
    SHIP_MODEL_ID,
    MODEL_QUANTITY,
    CROSSDOCK_TYPE,
    REQUIRED_QUANTITY,
    GRADE_CODE,
    SECONDARY_QUANTITY,
    SECONDARY_QUANTITY_DELIVERED,
    SECONDARY_QUANTITY_DETAILED,
    SECONDARY_REQUIRED_QUANTITY,
    SECONDARY_UOM_CODE,
    WIP_ENTITY_ID,
    REPETITIVE_SCHEDULE_ID,
    OPERATION_SEQ_NUM,
    WIP_SUPPLY_TYPE,
    MCC_CODE,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.mtl_txn_request_lines
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(LINE_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(LINE_ID, 0) AS LINE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.mtl_txn_request_lines
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(LINE_ID, 0)
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_txn_request_lines'
      )
    )
);