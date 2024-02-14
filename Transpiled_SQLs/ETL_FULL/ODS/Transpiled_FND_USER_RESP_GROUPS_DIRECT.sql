DROP TABLE IF EXISTS silver_bec_ods.fnd_user_resp_groups_direct;
CREATE TABLE IF NOT EXISTS silver_bec_ods.fnd_user_resp_groups_direct (
  USER_ID DECIMAL(15, 0),
  RESPONSIBILITY_ID DECIMAL(15, 0),
  RESPONSIBILITY_APPLICATION_ID DECIMAL(15, 0),
  SECURITY_GROUP_ID DECIMAL(15, 0),
  COMPONENT_ITEM_ID DECIMAL(15, 0),
  START_DATE TIMESTAMP,
  END_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  CREATION_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  DESCRIPTION STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.fnd_user_resp_groups_direct (
  USER_ID,
  RESPONSIBILITY_ID,
  RESPONSIBILITY_APPLICATION_ID,
  SECURITY_GROUP_ID,
  START_DATE,
  END_DATE,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  USER_ID,
  RESPONSIBILITY_ID,
  RESPONSIBILITY_APPLICATION_ID,
  SECURITY_GROUP_ID,
  START_DATE,
  END_DATE,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.fnd_user_resp_groups_direct;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fnd_user_resp_groups_direct';