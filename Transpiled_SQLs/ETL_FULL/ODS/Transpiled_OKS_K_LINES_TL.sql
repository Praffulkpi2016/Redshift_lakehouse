DROP TABLE IF EXISTS silver_bec_ods.OKS_K_LINES_TL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.OKS_K_LINES_TL (
  id STRING,
  `language` STRING,
  source_lang STRING,
  sfwt_flag STRING,
  invoice_text STRING,
  ib_trx_details STRING,
  status_text STRING,
  react_time_name STRING,
  security_group_id DECIMAL(15, 0),
  created_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_update_login DECIMAL(15, 0),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.OKS_K_LINES_TL (
  id,
  `language`,
  source_lang,
  sfwt_flag,
  invoice_text,
  ib_trx_details,
  status_text,
  react_time_name,
  security_group_id,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  id,
  `language`,
  source_lang,
  sfwt_flag,
  invoice_text,
  ib_trx_details,
  status_text,
  react_time_name,
  security_group_id,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.OKS_K_LINES_TL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oks_k_lines_tl';