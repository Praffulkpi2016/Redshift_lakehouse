DROP TABLE IF EXISTS silver_bec_ods.XXBEC_SRVCREV_CDW_MASTER_STG;
CREATE TABLE IF NOT EXISTS silver_bec_ods.XXBEC_SRVCREV_CDW_MASTER_STG (
  source STRING,
  contract_group STRING,
  contract_id BIGINT,
  ledger_id BIGINT,
  ledger_name STRING,
  org_id BIGINT,
  org_name STRING,
  site_id STRING,
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  last_update_date TIMESTAMP,
  extract_date TIMESTAMP,
  site_use_id DECIMAL(15, 0),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.XXBEC_SRVCREV_CDW_MASTER_STG (
  `source`,
  contract_group,
  contract_id,
  ledger_id,
  ledger_name,
  org_id,
  org_name,
  site_id,
  start_date,
  end_date,
  last_update_date,
  extract_date,
  site_use_id,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  `source`,
  contract_group,
  contract_id,
  ledger_id,
  ledger_name,
  org_id,
  org_name,
  site_id,
  start_date,
  end_date,
  last_update_date,
  extract_date,
  site_use_id,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.XXBEC_SRVCREV_CDW_MASTER_STG;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'xxbec_srvcrev_cdw_master_stg';