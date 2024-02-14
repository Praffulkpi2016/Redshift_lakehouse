/* Delete Records */
DELETE FROM silver_bec_ods.WIP_TRANSACTION_ACCOUNTS
WHERE
  (COALESCE(TRANSACTION_ID, 0), COALESCE(REFERENCE_ACCOUNT, 0), COALESCE(WIP_SUB_LEDGER_ID, 0)) IN (
    SELECT
      COALESCE(stg.TRANSACTION_ID, 0) AS TRANSACTION_ID,
      COALESCE(stg.REFERENCE_ACCOUNT, 0) AS REFERENCE_ACCOUNT,
      COALESCE(stg.WIP_SUB_LEDGER_ID, 0) AS WIP_SUB_LEDGER_ID
    FROM silver_bec_ods.WIP_TRANSACTION_ACCOUNTS AS ods, bronze_bec_ods_stg.WIP_TRANSACTION_ACCOUNTS AS stg
    WHERE
      COALESCE(ods.TRANSACTION_ID, 0) = COALESCE(stg.TRANSACTION_ID, 0)
      AND COALESCE(ods.REFERENCE_ACCOUNT, 0) = COALESCE(stg.REFERENCE_ACCOUNT, 0)
      AND COALESCE(ods.WIP_SUB_LEDGER_ID, 0) = COALESCE(stg.WIP_SUB_LEDGER_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.WIP_TRANSACTION_ACCOUNTS (
  TRANSACTION_ID,
  REFERENCE_ACCOUNT,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  ORGANIZATION_ID,
  TRANSACTION_DATE,
  WIP_ENTITY_ID,
  REPETITIVE_SCHEDULE_ID,
  ACCOUNTING_LINE_TYPE,
  TRANSACTION_VALUE,
  BASE_TRANSACTION_VALUE,
  CONTRA_SET_ID,
  PRIMARY_QUANTITY,
  RATE_OR_AMOUNT,
  BASIS_TYPE,
  RESOURCE_ID,
  COST_ELEMENT_ID,
  ACTIVITY_ID,
  CURRENCY_CODE,
  CURRENCY_CONVERSION_DATE,
  CURRENCY_CONVERSION_TYPE,
  CURRENCY_CONVERSION_RATE,
  OVERHEAD_BASIS_FACTOR,
  BASIS_RESOURCE_ID,
  GL_BATCH_ID,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  GL_SL_LINK_ID,
  WIP_SUB_LEDGER_ID,
  ENCUMBRANCE_TYPE_ID,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    TRANSACTION_ID,
    REFERENCE_ACCOUNT,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    ORGANIZATION_ID,
    TRANSACTION_DATE,
    WIP_ENTITY_ID,
    REPETITIVE_SCHEDULE_ID,
    ACCOUNTING_LINE_TYPE,
    TRANSACTION_VALUE,
    BASE_TRANSACTION_VALUE,
    CONTRA_SET_ID,
    PRIMARY_QUANTITY,
    RATE_OR_AMOUNT,
    BASIS_TYPE,
    RESOURCE_ID,
    COST_ELEMENT_ID,
    ACTIVITY_ID,
    CURRENCY_CODE,
    CURRENCY_CONVERSION_DATE,
    CURRENCY_CONVERSION_TYPE,
    CURRENCY_CONVERSION_RATE,
    OVERHEAD_BASIS_FACTOR,
    BASIS_RESOURCE_ID,
    GL_BATCH_ID,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    GL_SL_LINK_ID,
    WIP_SUB_LEDGER_ID,
    ENCUMBRANCE_TYPE_ID,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.WIP_TRANSACTION_ACCOUNTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(TRANSACTION_ID, 0), COALESCE(REFERENCE_ACCOUNT, 0), COALESCE(WIP_SUB_LEDGER_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(TRANSACTION_ID, 0) AS TRANSACTION_ID,
        COALESCE(REFERENCE_ACCOUNT, 0) AS REFERENCE_ACCOUNT,
        COALESCE(WIP_SUB_LEDGER_ID, 0) AS WIP_SUB_LEDGER_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.WIP_TRANSACTION_ACCOUNTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(TRANSACTION_ID, 0),
        COALESCE(REFERENCE_ACCOUNT, 0),
        COALESCE(WIP_SUB_LEDGER_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.WIP_TRANSACTION_ACCOUNTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.WIP_TRANSACTION_ACCOUNTS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(TRANSACTION_ID, 0), COALESCE(REFERENCE_ACCOUNT, 0), COALESCE(WIP_SUB_LEDGER_ID, 0)) IN (
    SELECT
      COALESCE(TRANSACTION_ID, 0),
      COALESCE(REFERENCE_ACCOUNT, 0),
      COALESCE(WIP_SUB_LEDGER_ID, 0)
    FROM bec_raw_dl_ext.WIP_TRANSACTION_ACCOUNTS
    WHERE
      (COALESCE(TRANSACTION_ID, 0), COALESCE(REFERENCE_ACCOUNT, 0), COALESCE(WIP_SUB_LEDGER_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(TRANSACTION_ID, 0),
          COALESCE(REFERENCE_ACCOUNT, 0),
          COALESCE(WIP_SUB_LEDGER_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.WIP_TRANSACTION_ACCOUNTS
        GROUP BY
          COALESCE(TRANSACTION_ID, 0),
          COALESCE(REFERENCE_ACCOUNT, 0),
          COALESCE(WIP_SUB_LEDGER_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'wip_transaction_accounts';