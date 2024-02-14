DROP TABLE IF EXISTS silver_bec_ods.GL_SEG_VAL_NORM_HIERARCHY;
CREATE TABLE IF NOT EXISTS silver_bec_ods.GL_SEG_VAL_NORM_HIERARCHY (
  flex_value_set_id DECIMAL(10, 0),
  parent_flex_value STRING,
  child_flex_value STRING,
  summary_flag STRING,
  last_updated_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_update_login DECIMAL(15, 0),
  created_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  status_code STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.GL_SEG_VAL_NORM_HIERARCHY (
  flex_value_set_id,
  parent_flex_value,
  child_flex_value,
  summary_flag,
  last_updated_by,
  last_update_date,
  last_update_login,
  created_by,
  creation_date,
  status_code,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  flex_value_set_id,
  parent_flex_value,
  child_flex_value,
  summary_flag,
  last_updated_by,
  last_update_date,
  last_update_login,
  created_by,
  creation_date,
  status_code,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_seg_val_norm_hierarchy';