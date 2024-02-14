DROP TABLE IF EXISTS silver_bec_ods.OE_TRANSACTION_TYPES_TL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.OE_TRANSACTION_TYPES_TL (
  transaction_type_id DECIMAL(15, 0),
  `language` STRING,
  source_lang STRING,
  name STRING,
  description STRING,
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  request_id DECIMAL(15, 0),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.OE_TRANSACTION_TYPES_TL (
  transaction_type_id,
  `language`,
  source_lang,
  `name`,
  description,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  program_application_id,
  program_id,
  request_id,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  transaction_type_id,
  `language`,
  source_lang,
  `name`,
  description,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  program_application_id,
  program_id,
  request_id,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.OE_TRANSACTION_TYPES_TL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oe_transaction_types_tl';