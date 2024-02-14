TRUNCATE table silver_bec_ods.fnd_user_resp_groups_direct;
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