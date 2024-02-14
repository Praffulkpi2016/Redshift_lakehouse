DROP TABLE IF EXISTS BEC_DWH.DIM_AR_BANK_ACC;
CREATE TABLE BEC_DWH.DIM_AR_BANK_ACC AS
SELECT
  acra.CASH_RECEIPT_ID,
  acra.ORG_ID,
  acra.RECEIPT_NUMBER,
  acra.DOC_SEQUENCE_VALUE,
  ACRA.RECEIPT_DATE,
  ACRA.COMMENTS,
  CBA.MASKED_ACCOUNT_NUM,
  'N' AS is_deleted_flg,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) AS source_app_id,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || COALESCE(acra.CASH_RECEIPT_ID, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM BEC_ODS.AR_CASH_RECEIPTS_ALL AS acra, BEC_ODS.CE_BANK_ACCOUNTS AS CBA, BEC_ODS.CE_BANK_ACCT_USES_ALL AS REMIT_BANK
WHERE
  1 = 1
  AND acra.REMIT_BANK_ACCT_USE_ID = REMIT_BANK.BANK_ACCT_USE_ID()
  AND acra.ORG_ID = REMIT_BANK.ORG_ID()
  AND remit_bank.bank_account_id = CBA.BANK_ACCOUNT_ID()
  AND acra.TYPE <> 'MISC';
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_bank_acc' AND batch_name = 'ar';