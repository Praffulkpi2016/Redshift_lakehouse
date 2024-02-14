DROP TABLE IF EXISTS silver_bec_ods.MSC_SUB_INVENTORIES;
CREATE TABLE IF NOT EXISTS silver_bec_ods.MSC_SUB_INVENTORIES (
  PLAN_ID DECIMAL(15, 0),
  ORGANIZATION_ID DECIMAL(15, 0),
  SUB_INVENTORY_CODE STRING,
  NETTING_TYPE DECIMAL(15, 0),
  SR_INSTANCE_ID DECIMAL(15, 0),
  DESCRIPTION STRING,
  REFRESH_NUMBER DECIMAL(15, 0),
  INVENTORY_ATP_CODE DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  REQUEST_ID DECIMAL(15, 0),
  PROGRAM_APPLICATION_ID DECIMAL(15, 0),
  PROGRAM_ID DECIMAL(15, 0),
  PROGRAM_UPDATE_DATE TIMESTAMP,
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
  INCLUDE_NON_NETTABLE DECIMAL(15, 0),
  SR_RESOURCE_NAME STRING,
  SR_CUSTOMER_ACCT_ID DECIMAL(15, 0),
  CONDITION_TYPE STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.MSC_SUB_INVENTORIES (
  PLAN_ID,
  ORGANIZATION_ID,
  SUB_INVENTORY_CODE,
  NETTING_TYPE,
  SR_INSTANCE_ID,
  DESCRIPTION,
  REFRESH_NUMBER,
  INVENTORY_ATP_CODE,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
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
  INCLUDE_NON_NETTABLE,
  SR_RESOURCE_NAME,
  SR_CUSTOMER_ACCT_ID,
  CONDITION_TYPE,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  PLAN_ID,
  ORGANIZATION_ID,
  SUB_INVENTORY_CODE,
  NETTING_TYPE,
  SR_INSTANCE_ID,
  DESCRIPTION,
  REFRESH_NUMBER,
  INVENTORY_ATP_CODE,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
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
  INCLUDE_NON_NETTABLE,
  SR_RESOURCE_NAME,
  SR_CUSTOMER_ACCT_ID,
  CONDITION_TYPE,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.MSC_SUB_INVENTORIES;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_sub_inventories';