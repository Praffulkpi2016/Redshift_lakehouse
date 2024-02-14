/* Delete Records */
DELETE FROM silver_bec_ods.mtl_supply
WHERE
  (SUPPLY_TYPE_CODE, SUPPLY_SOURCE_ID, COALESCE(PO_DISTRIBUTION_ID, 0)) IN (
    SELECT
      stg.SUPPLY_TYPE_CODE,
      stg.SUPPLY_SOURCE_ID,
      COALESCE(stg.PO_DISTRIBUTION_ID, 0)
    FROM silver_bec_ods.mtl_supply AS ods, bronze_bec_ods_stg.mtl_supply AS stg
    WHERE
      ods.SUPPLY_TYPE_CODE = stg.SUPPLY_TYPE_CODE
      AND ods.SUPPLY_SOURCE_ID = stg.SUPPLY_SOURCE_ID
      AND COALESCE(ods.PO_DISTRIBUTION_ID, 0) = COALESCE(stg.PO_DISTRIBUTION_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.mtl_supply (
  SUPPLY_TYPE_CODE,
  SUPPLY_SOURCE_ID,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  CREATED_BY,
  CREATION_DATE,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  REQ_HEADER_ID,
  REQ_LINE_ID,
  PO_HEADER_ID,
  PO_RELEASE_ID,
  PO_LINE_ID,
  PO_LINE_LOCATION_ID,
  PO_DISTRIBUTION_ID,
  SHIPMENT_HEADER_ID,
  SHIPMENT_LINE_ID,
  RCV_TRANSACTION_ID,
  ITEM_ID,
  ITEM_REVISION,
  CATEGORY_ID,
  QUANTITY,
  UNIT_OF_MEASURE,
  TO_ORG_PRIMARY_QUANTITY,
  TO_ORG_PRIMARY_UOM,
  RECEIPT_DATE,
  NEED_BY_DATE,
  EXPECTED_DELIVERY_DATE,
  DESTINATION_TYPE_CODE,
  LOCATION_ID,
  FROM_ORGANIZATION_ID,
  FROM_SUBINVENTORY,
  TO_ORGANIZATION_ID,
  TO_SUBINVENTORY,
  INTRANSIT_OWNING_ORG_ID,
  MRP_PRIMARY_QUANTITY,
  MRP_PRIMARY_UOM,
  MRP_EXPECTED_DELIVERY_DATE,
  MRP_DESTINATION_TYPE_CODE,
  MRP_TO_ORGANIZATION_ID,
  MRP_TO_SUBINVENTORY,
  CHANGE_FLAG,
  CHANGE_TYPE,
  COST_GROUP_ID,
  EXCLUDE_FROM_PLANNING,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    SUPPLY_TYPE_CODE,
    SUPPLY_SOURCE_ID,
    LAST_UPDATED_BY,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    CREATED_BY,
    CREATION_DATE,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    REQ_HEADER_ID,
    REQ_LINE_ID,
    PO_HEADER_ID,
    PO_RELEASE_ID,
    PO_LINE_ID,
    PO_LINE_LOCATION_ID,
    PO_DISTRIBUTION_ID,
    SHIPMENT_HEADER_ID,
    SHIPMENT_LINE_ID,
    RCV_TRANSACTION_ID,
    ITEM_ID,
    ITEM_REVISION,
    CATEGORY_ID,
    QUANTITY,
    UNIT_OF_MEASURE,
    TO_ORG_PRIMARY_QUANTITY,
    TO_ORG_PRIMARY_UOM,
    RECEIPT_DATE,
    NEED_BY_DATE,
    EXPECTED_DELIVERY_DATE,
    DESTINATION_TYPE_CODE,
    LOCATION_ID,
    FROM_ORGANIZATION_ID,
    FROM_SUBINVENTORY,
    TO_ORGANIZATION_ID,
    TO_SUBINVENTORY,
    INTRANSIT_OWNING_ORG_ID,
    MRP_PRIMARY_QUANTITY,
    MRP_PRIMARY_UOM,
    MRP_EXPECTED_DELIVERY_DATE,
    MRP_DESTINATION_TYPE_CODE,
    MRP_TO_ORGANIZATION_ID,
    MRP_TO_SUBINVENTORY,
    CHANGE_FLAG,
    CHANGE_TYPE,
    COST_GROUP_ID,
    EXCLUDE_FROM_PLANNING,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.mtl_supply
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (SUPPLY_TYPE_CODE, SUPPLY_SOURCE_ID, COALESCE(PO_DISTRIBUTION_ID, 0), kca_seq_id) IN (
      SELECT
        SUPPLY_TYPE_CODE,
        SUPPLY_SOURCE_ID,
        COALESCE(PO_DISTRIBUTION_ID, 0),
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.mtl_supply
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        SUPPLY_TYPE_CODE,
        SUPPLY_SOURCE_ID,
        COALESCE(PO_DISTRIBUTION_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.mtl_supply SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.mtl_supply SET IS_DELETED_FLG = 'Y'
WHERE
  (SUPPLY_TYPE_CODE, SUPPLY_SOURCE_ID, COALESCE(PO_DISTRIBUTION_ID, 0)) IN (
    SELECT
      SUPPLY_TYPE_CODE,
      SUPPLY_SOURCE_ID,
      COALESCE(PO_DISTRIBUTION_ID, 0)
    FROM bec_raw_dl_ext.mtl_supply
    WHERE
      (SUPPLY_TYPE_CODE, SUPPLY_SOURCE_ID, COALESCE(PO_DISTRIBUTION_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          SUPPLY_TYPE_CODE,
          SUPPLY_SOURCE_ID,
          COALESCE(PO_DISTRIBUTION_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.mtl_supply
        GROUP BY
          SUPPLY_TYPE_CODE,
          SUPPLY_SOURCE_ID,
          COALESCE(PO_DISTRIBUTION_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_supply';