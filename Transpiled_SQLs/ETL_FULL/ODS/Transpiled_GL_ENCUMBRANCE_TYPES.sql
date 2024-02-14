DROP TABLE IF EXISTS silver_bec_ods.GL_ENCUMBRANCE_TYPES;
CREATE TABLE IF NOT EXISTS silver_bec_ods.GL_ENCUMBRANCE_TYPES (
  encumbrance_type_id DECIMAL(15, 0),
  encumbrance_type STRING,
  enabled_flag STRING,
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  description STRING,
  encumbrance_type_key STRING,
  zd_edition_name STRING,
  zd_sync STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.GL_ENCUMBRANCE_TYPES (
  encumbrance_type_id,
  encumbrance_type,
  enabled_flag,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  description,
  encumbrance_type_key,
  zd_edition_name,
  zd_sync,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  encumbrance_type_id,
  encumbrance_type,
  enabled_flag,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  description,
  encumbrance_type_key,
  zd_edition_name,
  zd_sync,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.GL_ENCUMBRANCE_TYPES;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_encumbrance_types';