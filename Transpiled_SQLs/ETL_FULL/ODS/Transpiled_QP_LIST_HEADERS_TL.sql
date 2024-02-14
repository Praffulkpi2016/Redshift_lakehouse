DROP TABLE IF EXISTS silver_bec_ods.QP_LIST_HEADERS_TL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.QP_LIST_HEADERS_TL (
  list_header_id DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  `language` STRING,
  source_lang STRING,
  name STRING,
  description STRING,
  version_no STRING,
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.QP_LIST_HEADERS_TL (
  list_header_id,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  `language`,
  source_lang,
  `name`,
  description,
  version_no,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  list_header_id,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  `language`,
  source_lang,
  `name`,
  description,
  version_no,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.QP_LIST_HEADERS_TL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'qp_list_headers_tl';