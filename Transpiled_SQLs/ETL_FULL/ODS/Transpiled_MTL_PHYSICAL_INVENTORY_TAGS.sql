DROP TABLE IF EXISTS silver_bec_ods.MTL_PHYSICAL_INVENTORY_TAGS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.MTL_PHYSICAL_INVENTORY_TAGS (
  TAG_ID DECIMAL(15, 0),
  PHYSICAL_INVENTORY_ID DECIMAL(15, 0),
  ORGANIZATION_ID DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  VOID_FLAG DECIMAL(15, 0),
  TAG_NUMBER STRING,
  ADJUSTMENT_ID DECIMAL(15, 0),
  INVENTORY_ITEM_ID DECIMAL(15, 0),
  TAG_QUANTITY DECIMAL(28, 10),
  TAG_UOM STRING,
  TAG_QUANTITY_AT_STANDARD_UOM DECIMAL(28, 10),
  STANDARD_UOM STRING,
  SUBINVENTORY STRING,
  LOCATOR_ID DECIMAL(15, 0),
  LOT_NUMBER STRING,
  LOT_EXPIRATION_DATE TIMESTAMP,
  REVISION STRING,
  SERIAL_NUM STRING,
  COUNTED_BY_EMPLOYEE_ID DECIMAL(9, 0),
  LOT_SERIAL_CONTROLS STRING,
  ATTRIBUTE_CATEGORY STRING,
  ATTRIBUTE1 STRING,
  ATTRIBUTE2 STRING,
  ATTRIBUTE3 STRING,
  ATTRIBUTE4 STRING,
  ATTRIBUTE5 STRING,
  ATTRIBUTE6 STRING,
  ATTRIBUTE7 STRING,
  ATTRIBUTE8 STRING,
  ATTRIBUTE9 STRING,
  ATTRIBUTE10 STRING,
  ATTRIBUTE11 STRING,
  ATTRIBUTE12 STRING,
  ATTRIBUTE13 STRING,
  ATTRIBUTE14 STRING,
  ATTRIBUTE15 STRING,
  REQUEST_ID DECIMAL(15, 0),
  PROGRAM_APPLICATION_ID DECIMAL(15, 0),
  PROGRAM_ID DECIMAL(15, 0),
  PROGRAM_UPDATE_DATE TIMESTAMP,
  PARENT_LPN_ID DECIMAL(15, 0),
  OUTERMOST_LPN_ID DECIMAL(15, 0),
  COST_GROUP_ID DECIMAL(15, 0),
  TAG_SECONDARY_QUANTITY DECIMAL(28, 10),
  TAG_SECONDARY_UOM STRING,
  TAG_QTY_AT_STD_SECONDARY_UOM DECIMAL(28, 10),
  STANDARD_SECONDARY_UOM STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.MTL_PHYSICAL_INVENTORY_TAGS (
  TAG_ID,
  PHYSICAL_INVENTORY_ID,
  ORGANIZATION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  VOID_FLAG,
  TAG_NUMBER,
  ADJUSTMENT_ID,
  INVENTORY_ITEM_ID,
  TAG_QUANTITY,
  TAG_UOM,
  TAG_QUANTITY_AT_STANDARD_UOM,
  STANDARD_UOM,
  SUBINVENTORY,
  LOCATOR_ID,
  LOT_NUMBER,
  LOT_EXPIRATION_DATE,
  REVISION,
  SERIAL_NUM,
  COUNTED_BY_EMPLOYEE_ID,
  LOT_SERIAL_CONTROLS,
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
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  PARENT_LPN_ID,
  OUTERMOST_LPN_ID,
  COST_GROUP_ID,
  TAG_SECONDARY_QUANTITY,
  TAG_SECONDARY_UOM,
  TAG_QTY_AT_STD_SECONDARY_UOM,
  STANDARD_SECONDARY_UOM,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  TAG_ID,
  PHYSICAL_INVENTORY_ID,
  ORGANIZATION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  VOID_FLAG,
  TAG_NUMBER,
  ADJUSTMENT_ID,
  INVENTORY_ITEM_ID,
  TAG_QUANTITY,
  TAG_UOM,
  TAG_QUANTITY_AT_STANDARD_UOM,
  STANDARD_UOM,
  SUBINVENTORY,
  LOCATOR_ID,
  LOT_NUMBER,
  LOT_EXPIRATION_DATE,
  REVISION,
  SERIAL_NUM,
  COUNTED_BY_EMPLOYEE_ID,
  LOT_SERIAL_CONTROLS,
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
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  PARENT_LPN_ID,
  OUTERMOST_LPN_ID,
  COST_GROUP_ID,
  TAG_SECONDARY_QUANTITY,
  TAG_SECONDARY_UOM,
  TAG_QTY_AT_STD_SECONDARY_UOM,
  STANDARD_SECONDARY_UOM,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.MTL_PHYSICAL_INVENTORY_TAGS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_physical_inventory_tags';