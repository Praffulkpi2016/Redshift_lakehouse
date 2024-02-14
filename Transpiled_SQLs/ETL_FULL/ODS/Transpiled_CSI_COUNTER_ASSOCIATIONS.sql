DROP TABLE IF EXISTS silver_bec_ods.CSI_COUNTER_ASSOCIATIONS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.CSI_COUNTER_ASSOCIATIONS (
  INSTANCE_ASSOCIATION_ID DECIMAL(15, 0),
  SOURCE_OBJECT_CODE STRING,
  SOURCE_OBJECT_ID DECIMAL(15, 0),
  COUNTER_ID DECIMAL(15, 0),
  START_DATE_ACTIVE TIMESTAMP,
  END_DATE_ACTIVE TIMESTAMP,
  SECURITY_GROUP_ID DECIMAL(15, 0),
  OBJECT_VERSION_NUMBER DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
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
  MIGRATED_FLAG STRING,
  MAINT_ORGANIZATION_ID DECIMAL(15, 0),
  PRIMARY_FAILURE_FLAG STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.CSI_COUNTER_ASSOCIATIONS (
  INSTANCE_ASSOCIATION_ID,
  SOURCE_OBJECT_CODE,
  SOURCE_OBJECT_ID,
  COUNTER_ID,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  SECURITY_GROUP_ID,
  OBJECT_VERSION_NUMBER,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
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
  MIGRATED_FLAG,
  MAINT_ORGANIZATION_ID,
  PRIMARY_FAILURE_FLAG,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  INSTANCE_ASSOCIATION_ID,
  SOURCE_OBJECT_CODE,
  SOURCE_OBJECT_ID,
  COUNTER_ID,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  SECURITY_GROUP_ID,
  OBJECT_VERSION_NUMBER,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
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
  MIGRATED_FLAG,
  MAINT_ORGANIZATION_ID,
  PRIMARY_FAILURE_FLAG,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.CSI_COUNTER_ASSOCIATIONS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'csi_counter_associations';