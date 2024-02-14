TRUNCATE table bronze_bec_ods_stg.XXBEC_SRVCREV_CDW_MASTER_STG;
INSERT INTO bronze_bec_ods_stg.XXBEC_SRVCREV_CDW_MASTER_STG (
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
  kca_operation,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
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
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.XXBEC_SRVCREV_CDW_MASTER_STG
);