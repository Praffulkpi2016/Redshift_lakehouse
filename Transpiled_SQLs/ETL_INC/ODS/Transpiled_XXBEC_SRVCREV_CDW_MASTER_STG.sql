TRUNCATE table silver_bec_ods.XXBEC_SRVCREV_CDW_MASTER_STG;
/* Insert records */
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
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
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
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.XXBEC_SRVCREV_CDW_MASTER_STG
);
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'xxbec_srvcrev_cdw_master_stg';