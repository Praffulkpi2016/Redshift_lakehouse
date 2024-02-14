/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_AR_BANK_ACC
WHERE
  (
    COALESCE(CASH_RECEIPT_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.CASH_RECEIPT_ID, 0)
    FROM gold_bec_dwh.DIM_AR_BANK_ACC AS dw, (
      SELECT
        acra.CASH_RECEIPT_ID,
        acra.last_update_date
      FROM BEC_ODS.AR_CASH_RECEIPTS_ALL AS acra, BEC_ODS.CE_BANK_ACCOUNTS AS CBA, BEC_ODS.CE_BANK_ACCT_USES_ALL AS REMIT_BANK
      WHERE
        1 = 1
        AND acra.REMIT_BANK_ACCT_USE_ID = REMIT_BANK.BANK_ACCT_USE_ID()
        AND acra.ORG_ID = REMIT_BANK.ORG_ID()
        AND remit_bank.bank_account_id = CBA.BANK_ACCOUNT_ID()
        AND acra.TYPE <> 'MISC'
        AND (
          acra.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_ar_bank_acc' AND batch_name = 'ar'
          )
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.CASH_RECEIPT_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_AR_BANK_ACC (
  cash_receipt_id,
  org_id,
  receipt_number,
  doc_sequence_value,
  receipt_date,
  comments,
  masked_account_num,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
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
    AND acra.TYPE <> 'MISC'
    AND (
      acra.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_ar_bank_acc' AND batch_name = 'ar'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_AR_BANK_ACC SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(CASH_RECEIPT_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.CASH_RECEIPT_ID, 0)
    FROM gold_bec_dwh.DIM_AR_BANK_ACC AS dw, (
      SELECT
        acra.CASH_RECEIPT_ID,
        acra.last_update_date,
        acra.kca_operation
      FROM (
        SELECT
          *
        FROM silver_bec_ods.AR_CASH_RECEIPTS_ALL
        WHERE
          is_deleted_flg <> 'Y'
      ) AS acra, (
        SELECT
          *
        FROM silver_bec_ods.CE_BANK_ACCOUNTS
        WHERE
          is_deleted_flg <> 'Y'
      ) AS CBA, (
        SELECT
          *
        FROM silver_bec_ods.CE_BANK_ACCT_USES_ALL
        WHERE
          is_deleted_flg <> 'Y'
      ) AS REMIT_BANK
      WHERE
        1 = 1
        AND acra.REMIT_BANK_ACCT_USE_ID = REMIT_BANK.BANK_ACCT_USE_ID()
        AND acra.ORG_ID = REMIT_BANK.ORG_ID()
        AND remit_bank.bank_account_id = CBA.BANK_ACCOUNT_ID()
        AND acra.TYPE <> 'MISC'
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.CASH_RECEIPT_ID, 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ar_bank_acc' AND batch_name = 'ar';