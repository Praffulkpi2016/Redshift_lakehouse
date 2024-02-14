DROP TABLE IF EXISTS silver_bec_ods.JTF_TASKS_TL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.JTF_TASKS_TL (
  TASK_ID DECIMAL(15, 0),
  LANGUAGE STRING,
  SOURCE_LANG STRING,
  TASK_NAME STRING,
  DESCRIPTION STRING,
  CREATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  SECURITY_GROUP_ID DECIMAL(15, 0),
  REJECTION_MESSAGE STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.JTF_TASKS_TL (
  TASK_ID,
  LANGUAGE,
  SOURCE_LANG,
  TASK_NAME,
  DESCRIPTION,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  SECURITY_GROUP_ID,
  REJECTION_MESSAGE,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  TASK_ID,
  LANGUAGE,
  SOURCE_LANG,
  TASK_NAME,
  DESCRIPTION,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  SECURITY_GROUP_ID,
  REJECTION_MESSAGE,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.JTF_TASKS_TL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'jtf_tasks_tl';