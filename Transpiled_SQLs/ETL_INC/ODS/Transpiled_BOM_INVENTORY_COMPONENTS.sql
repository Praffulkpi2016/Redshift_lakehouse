TRUNCATE table silver_bec_ods.BOM_INVENTORY_COMPONENTS;
INSERT INTO silver_bec_ods.bom_inventory_components (
  OPERATION_SEQ_NUM,
  COMPONENT_ITEM_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  ITEM_NUM,
  BASIS_TYPE,
  COMPONENT_QUANTITY,
  COMPONENT_YIELD_FACTOR,
  COMPONENT_REMARKS,
  EFFECTIVITY_DATE,
  CHANGE_NOTICE,
  IMPLEMENTATION_DATE,
  DISABLE_DATE,
  ATTRIBUTE_CATEGORY,
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
  PLANNING_FACTOR,
  QUANTITY_RELATED,
  SO_BASIS,
  OPTIONAL,
  MUTUALLY_EXCLUSIVE_OPTIONS,
  INCLUDE_IN_COST_ROLLUP,
  CHECK_ATP,
  SHIPPING_ALLOWED,
  REQUIRED_TO_SHIP,
  REQUIRED_FOR_REVENUE,
  INCLUDE_ON_SHIP_DOCS,
  INCLUDE_ON_BILL_DOCS,
  LOW_QUANTITY,
  HIGH_QUANTITY,
  ACD_TYPE,
  OLD_COMPONENT_SEQUENCE_ID,
  COMPONENT_SEQUENCE_ID,
  BILL_SEQUENCE_ID,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  WIP_SUPPLY_TYPE,
  PICK_COMPONENTS,
  SUPPLY_SUBINVENTORY,
  SUPPLY_LOCATOR_ID,
  OPERATION_LEAD_TIME_PERCENT,
  REVISED_ITEM_SEQUENCE_ID,
  COST_FACTOR,
  BOM_ITEM_TYPE,
  FROM_END_ITEM_UNIT_NUMBER,
  TO_END_ITEM_UNIT_NUMBER,
  COMPONENT_ITEM_REVISION_ID,
  ORIGINAL_SYSTEM_REFERENCE,
  ECO_FOR_PRODUCTION,
  DELETE_GROUP_NAME,
  DG_DESCRIPTION,
  ENFORCE_INT_REQUIREMENTS,
  OPTIONAL_ON_MODEL,
  PARENT_BILL_SEQ_ID,
  MODEL_COMP_SEQ_ID,
  PLAN_LEVEL,
  AUTO_REQUEST_MATERIAL,
  SUGGESTED_VENDOR_NAME,
  VENDOR_ID,
  UNIT_PRICE,
  OBJ_NAME,
  PK1_VALUE,
  PK2_VALUE,
  PK3_VALUE,
  PK4_VALUE,
  PK5_VALUE,
  FROM_END_ITEM_REV_ID,
  TO_END_ITEM_REV_ID,
  OVERLAPPING_CHANGES,
  FROM_OBJECT_REVISION_ID,
  FROM_MINOR_REVISION_ID,
  TO_OBJECT_REVISION_ID,
  TO_MINOR_REVISION_ID,
  FROM_END_ITEM_MINOR_REV_ID,
  TO_END_ITEM_MINOR_REV_ID,
  COMPONENT_MINOR_REVISION_ID,
  FROM_BILL_REVISION_ID,
  TO_BILL_REVISION_ID,
  FROM_STRUCTURE_REVISION_CODE,
  TO_STRUCTURE_REVISION_CODE,
  FROM_END_ITEM_STRC_REV_ID,
  TO_END_ITEM_STRC_REV_ID,
  COMMON_COMPONENT_SEQUENCE_ID,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    OPERATION_SEQ_NUM,
    COMPONENT_ITEM_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    ITEM_NUM,
    BASIS_TYPE,
    COMPONENT_QUANTITY,
    COMPONENT_YIELD_FACTOR,
    COMPONENT_REMARKS,
    EFFECTIVITY_DATE,
    CHANGE_NOTICE,
    IMPLEMENTATION_DATE,
    DISABLE_DATE,
    ATTRIBUTE_CATEGORY,
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
    PLANNING_FACTOR,
    QUANTITY_RELATED,
    SO_BASIS,
    OPTIONAL,
    MUTUALLY_EXCLUSIVE_OPTIONS,
    INCLUDE_IN_COST_ROLLUP,
    CHECK_ATP,
    SHIPPING_ALLOWED,
    REQUIRED_TO_SHIP,
    REQUIRED_FOR_REVENUE,
    INCLUDE_ON_SHIP_DOCS,
    INCLUDE_ON_BILL_DOCS,
    LOW_QUANTITY,
    HIGH_QUANTITY,
    ACD_TYPE,
    OLD_COMPONENT_SEQUENCE_ID,
    COMPONENT_SEQUENCE_ID,
    BILL_SEQUENCE_ID,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    WIP_SUPPLY_TYPE,
    PICK_COMPONENTS,
    SUPPLY_SUBINVENTORY,
    SUPPLY_LOCATOR_ID,
    OPERATION_LEAD_TIME_PERCENT,
    REVISED_ITEM_SEQUENCE_ID,
    COST_FACTOR,
    BOM_ITEM_TYPE,
    FROM_END_ITEM_UNIT_NUMBER,
    TO_END_ITEM_UNIT_NUMBER,
    COMPONENT_ITEM_REVISION_ID,
    ORIGINAL_SYSTEM_REFERENCE,
    ECO_FOR_PRODUCTION,
    DELETE_GROUP_NAME,
    DG_DESCRIPTION,
    ENFORCE_INT_REQUIREMENTS,
    OPTIONAL_ON_MODEL,
    PARENT_BILL_SEQ_ID,
    MODEL_COMP_SEQ_ID,
    PLAN_LEVEL,
    AUTO_REQUEST_MATERIAL,
    SUGGESTED_VENDOR_NAME,
    VENDOR_ID,
    UNIT_PRICE,
    OBJ_NAME,
    PK1_VALUE,
    PK2_VALUE,
    PK3_VALUE,
    PK4_VALUE,
    PK5_VALUE,
    FROM_END_ITEM_REV_ID,
    TO_END_ITEM_REV_ID,
    OVERLAPPING_CHANGES,
    FROM_OBJECT_REVISION_ID,
    FROM_MINOR_REVISION_ID,
    TO_OBJECT_REVISION_ID,
    TO_MINOR_REVISION_ID,
    FROM_END_ITEM_MINOR_REV_ID,
    TO_END_ITEM_MINOR_REV_ID,
    COMPONENT_MINOR_REVISION_ID,
    FROM_BILL_REVISION_ID,
    TO_BILL_REVISION_ID,
    FROM_STRUCTURE_REVISION_CODE,
    TO_STRUCTURE_REVISION_CODE,
    FROM_END_ITEM_STRC_REV_ID,
    TO_END_ITEM_STRC_REV_ID,
    COMMON_COMPONENT_SEQUENCE_ID,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.bom_inventory_components
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'bom_inventory_components';