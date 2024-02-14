DROP TABLE IF EXISTS silver_bec_ods.mtl_interorg_parameters;
CREATE TABLE IF NOT EXISTS silver_bec_ods.mtl_interorg_parameters (
  FROM_ORGANIZATION_ID DECIMAL(15, 0),
  TO_ORGANIZATION_ID DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  INTRANSIT_TYPE DECIMAL(15, 0),
  DISTANCE_UOM_CODE STRING,
  TO_ORGANIZATION_DISTANCE DECIMAL(28, 10),
  FOB_POINT DECIMAL(15, 0),
  MATL_INTERORG_TRANSFER_CODE DECIMAL(15, 0),
  ROUTING_HEADER_ID DECIMAL(15, 0),
  INTERNAL_ORDER_REQUIRED_FLAG DECIMAL(15, 0),
  INTRANSIT_INV_ACCOUNT DECIMAL(15, 0),
  INTERORG_TRNSFR_CHARGE_PERCENT DECIMAL(28, 10),
  INTERORG_TRANSFER_CR_ACCOUNT DECIMAL(15, 0),
  INTERORG_RECEIVABLES_ACCOUNT DECIMAL(15, 0),
  INTERORG_PAYABLES_ACCOUNT DECIMAL(28, 10),
  INTERORG_PRICE_VAR_ACCOUNT DECIMAL(15, 0),
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
  GLOBAL_ATTRIBUTE_CATEGORY STRING,
  GLOBAL_ATTRIBUTE1 STRING,
  GLOBAL_ATTRIBUTE2 STRING,
  GLOBAL_ATTRIBUTE3 STRING,
  GLOBAL_ATTRIBUTE4 STRING,
  GLOBAL_ATTRIBUTE5 STRING,
  GLOBAL_ATTRIBUTE6 STRING,
  GLOBAL_ATTRIBUTE7 STRING,
  GLOBAL_ATTRIBUTE8 STRING,
  GLOBAL_ATTRIBUTE9 STRING,
  GLOBAL_ATTRIBUTE10 STRING,
  GLOBAL_ATTRIBUTE11 STRING,
  GLOBAL_ATTRIBUTE12 STRING,
  GLOBAL_ATTRIBUTE13 STRING,
  GLOBAL_ATTRIBUTE14 STRING,
  GLOBAL_ATTRIBUTE15 STRING,
  GLOBAL_ATTRIBUTE16 STRING,
  GLOBAL_ATTRIBUTE17 STRING,
  GLOBAL_ATTRIBUTE18 STRING,
  GLOBAL_ATTRIBUTE19 STRING,
  GLOBAL_ATTRIBUTE20 STRING,
  ELEMENTAL_VISIBILITY_ENABLED STRING,
  MANUAL_RECEIPT_EXPENSE STRING,
  PROFIT_IN_INV_ACCOUNT DECIMAL(15, 0),
  SHIKYU_ENABLED_FLAG STRING,
  SHIKYU_DEFAULT_ORDER_TYPE_ID DECIMAL(15, 0),
  SHIKYU_OEM_VAR_ACCOUNT_ID DECIMAL(15, 0),
  SHIKYU_TP_OFFSET_ACCOUNT_ID DECIMAL(15, 0),
  INTERORG_PROFIT_ACCOUNT DECIMAL(28, 10),
  PRICELIST_ID DECIMAL(15, 0),
  SUBCONTRACTING_TYPE STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.mtl_interorg_parameters (
  FROM_ORGANIZATION_ID,
  TO_ORGANIZATION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  INTRANSIT_TYPE,
  DISTANCE_UOM_CODE,
  TO_ORGANIZATION_DISTANCE,
  FOB_POINT,
  MATL_INTERORG_TRANSFER_CODE,
  ROUTING_HEADER_ID,
  INTERNAL_ORDER_REQUIRED_FLAG,
  INTRANSIT_INV_ACCOUNT,
  INTERORG_TRNSFR_CHARGE_PERCENT,
  INTERORG_TRANSFER_CR_ACCOUNT,
  INTERORG_RECEIVABLES_ACCOUNT,
  INTERORG_PAYABLES_ACCOUNT,
  INTERORG_PRICE_VAR_ACCOUNT,
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
  GLOBAL_ATTRIBUTE_CATEGORY,
  GLOBAL_ATTRIBUTE1,
  GLOBAL_ATTRIBUTE2,
  GLOBAL_ATTRIBUTE3,
  GLOBAL_ATTRIBUTE4,
  GLOBAL_ATTRIBUTE5,
  GLOBAL_ATTRIBUTE6,
  GLOBAL_ATTRIBUTE7,
  GLOBAL_ATTRIBUTE8,
  GLOBAL_ATTRIBUTE9,
  GLOBAL_ATTRIBUTE10,
  GLOBAL_ATTRIBUTE11,
  GLOBAL_ATTRIBUTE12,
  GLOBAL_ATTRIBUTE13,
  GLOBAL_ATTRIBUTE14,
  GLOBAL_ATTRIBUTE15,
  GLOBAL_ATTRIBUTE16,
  GLOBAL_ATTRIBUTE17,
  GLOBAL_ATTRIBUTE18,
  GLOBAL_ATTRIBUTE19,
  GLOBAL_ATTRIBUTE20,
  ELEMENTAL_VISIBILITY_ENABLED,
  MANUAL_RECEIPT_EXPENSE,
  PROFIT_IN_INV_ACCOUNT,
  SHIKYU_ENABLED_FLAG,
  SHIKYU_DEFAULT_ORDER_TYPE_ID,
  SHIKYU_OEM_VAR_ACCOUNT_ID,
  SHIKYU_TP_OFFSET_ACCOUNT_ID,
  INTERORG_PROFIT_ACCOUNT,
  PRICELIST_ID,
  SUBCONTRACTING_TYPE,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  FROM_ORGANIZATION_ID,
  TO_ORGANIZATION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  INTRANSIT_TYPE,
  DISTANCE_UOM_CODE,
  TO_ORGANIZATION_DISTANCE,
  FOB_POINT,
  MATL_INTERORG_TRANSFER_CODE,
  ROUTING_HEADER_ID,
  INTERNAL_ORDER_REQUIRED_FLAG,
  INTRANSIT_INV_ACCOUNT,
  INTERORG_TRNSFR_CHARGE_PERCENT,
  INTERORG_TRANSFER_CR_ACCOUNT,
  INTERORG_RECEIVABLES_ACCOUNT,
  INTERORG_PAYABLES_ACCOUNT,
  INTERORG_PRICE_VAR_ACCOUNT,
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
  GLOBAL_ATTRIBUTE_CATEGORY,
  GLOBAL_ATTRIBUTE1,
  GLOBAL_ATTRIBUTE2,
  GLOBAL_ATTRIBUTE3,
  GLOBAL_ATTRIBUTE4,
  GLOBAL_ATTRIBUTE5,
  GLOBAL_ATTRIBUTE6,
  GLOBAL_ATTRIBUTE7,
  GLOBAL_ATTRIBUTE8,
  GLOBAL_ATTRIBUTE9,
  GLOBAL_ATTRIBUTE10,
  GLOBAL_ATTRIBUTE11,
  GLOBAL_ATTRIBUTE12,
  GLOBAL_ATTRIBUTE13,
  GLOBAL_ATTRIBUTE14,
  GLOBAL_ATTRIBUTE15,
  GLOBAL_ATTRIBUTE16,
  GLOBAL_ATTRIBUTE17,
  GLOBAL_ATTRIBUTE18,
  GLOBAL_ATTRIBUTE19,
  GLOBAL_ATTRIBUTE20,
  ELEMENTAL_VISIBILITY_ENABLED,
  MANUAL_RECEIPT_EXPENSE,
  PROFIT_IN_INV_ACCOUNT,
  SHIKYU_ENABLED_FLAG,
  SHIKYU_DEFAULT_ORDER_TYPE_ID,
  SHIKYU_OEM_VAR_ACCOUNT_ID,
  SHIKYU_TP_OFFSET_ACCOUNT_ID,
  INTERORG_PROFIT_ACCOUNT,
  PRICELIST_ID,
  SUBCONTRACTING_TYPE,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.mtl_interorg_parameters;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_interorg_parameters';