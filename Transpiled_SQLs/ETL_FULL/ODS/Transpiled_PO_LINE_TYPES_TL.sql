DROP TABLE IF EXISTS silver_bec_ods.PO_LINE_TYPES_TL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.PO_LINE_TYPES_TL (
  line_type_id DECIMAL(15, 0),
  `language` STRING,
  source_lang STRING,
  description STRING,
  line_type STRING,
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  zd_edition_name STRING,
  zd_sync STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.PO_LINE_TYPES_TL (
  line_type_id,
  `language`,
  source_lang,
  description,
  line_type,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  ZD_EDITION_NAME,
  ZD_SYNC,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  line_type_id,
  `language`,
  source_lang,
  description,
  line_type,
  last_update_date,
  last_updated_by,
  last_update_login,
  creation_date,
  created_by,
  ZD_EDITION_NAME,
  ZD_SYNC,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.PO_LINE_TYPES_TL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_line_types_tl';