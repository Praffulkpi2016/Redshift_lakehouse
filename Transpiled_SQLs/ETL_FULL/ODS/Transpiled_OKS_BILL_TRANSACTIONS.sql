DROP TABLE IF EXISTS silver_bec_ods.OKS_BILL_TRANSACTIONS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.OKS_BILL_TRANSACTIONS (
  id STRING,
  currency_code STRING,
  object_version_number DECIMAL(15, 0),
  created_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  trx_date TIMESTAMP,
  trx_number STRING,
  trx_amount DECIMAL(28, 10),
  trx_class STRING,
  last_update_login DECIMAL(15, 0),
  security_group_id DECIMAL(15, 0),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.OKS_BILL_TRANSACTIONS (
  id,
  currency_code,
  object_version_number,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  trx_date,
  trx_number,
  trx_amount,
  trx_class,
  last_update_login,
  security_group_id,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  id,
  currency_code,
  object_version_number,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  trx_date,
  trx_number,
  trx_amount,
  trx_class,
  last_update_login,
  security_group_id,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.OKS_BILL_TRANSACTIONS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'oks_bill_transactions';