DROP TABLE IF EXISTS silver_bec_ods.jtf_rs_resource_extns_tl;
CREATE TABLE IF NOT EXISTS silver_bec_ods.jtf_rs_resource_extns_tl (
  created_by DECIMAL(15, 0),
  resource_id DECIMAL(15, 0),
  category STRING,
  creation_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_update_login DECIMAL(15, 0),
  `language` STRING,
  resource_name STRING,
  source_lang STRING,
  security_group_id DECIMAL(15, 0),
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.jtf_rs_resource_extns_tl (
  created_by,
  resource_id,
  category,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  `language`,
  resource_name,
  source_lang,
  security_group_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  created_by,
  resource_id,
  category,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  `language`,
  resource_name,
  source_lang,
  security_group_id,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.jtf_rs_resource_extns_tl;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'jtf_rs_resource_extns_tl';