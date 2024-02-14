DROP TABLE IF EXISTS silver_bec_ods.CSF_DEBRIEF_HEADERS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.CSF_DEBRIEF_HEADERS (
  DEBRIEF_HEADER_ID DECIMAL(15, 0),
  DEBRIEF_NUMBER STRING,
  DEBRIEF_DATE TIMESTAMP,
  DEBRIEF_STATUS_ID DECIMAL(15, 0),
  TASK_ASSIGNMENT_ID DECIMAL(15, 0),
  CREATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
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
  ATTRIBUTE_CATEGORY STRING,
  SECURITY_GROUP_ID DECIMAL(15, 0),
  STATISTICS_UPDATED STRING,
  PROCESSED_FLAG STRING,
  TRAVEL_START_TIME TIMESTAMP,
  TRAVEL_END_TIME TIMESTAMP,
  OBJECT_VERSION_NUMBER DECIMAL(15, 0),
  TRAVEL_DISTANCE_IN_KM DECIMAL(28, 10),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.CSF_DEBRIEF_HEADERS (
  DEBRIEF_HEADER_ID,
  DEBRIEF_NUMBER,
  DEBRIEF_DATE,
  DEBRIEF_STATUS_ID,
  TASK_ASSIGNMENT_ID,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
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
  SECURITY_GROUP_ID,
  STATISTICS_UPDATED,
  PROCESSED_FLAG,
  TRAVEL_START_TIME,
  TRAVEL_END_TIME,
  OBJECT_VERSION_NUMBER,
  TRAVEL_DISTANCE_IN_KM,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  DEBRIEF_HEADER_ID,
  DEBRIEF_NUMBER,
  DEBRIEF_DATE,
  DEBRIEF_STATUS_ID,
  TASK_ASSIGNMENT_ID,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
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
  SECURITY_GROUP_ID,
  STATISTICS_UPDATED,
  PROCESSED_FLAG,
  TRAVEL_START_TIME,
  TRAVEL_END_TIME,
  OBJECT_VERSION_NUMBER,
  TRAVEL_DISTANCE_IN_KM,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.CSF_DEBRIEF_HEADERS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'csf_debrief_headers';