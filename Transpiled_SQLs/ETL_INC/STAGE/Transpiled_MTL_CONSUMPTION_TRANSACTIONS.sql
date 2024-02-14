TRUNCATE table bronze_bec_ods_stg.mtl_consumption_transactions;
INSERT INTO bronze_bec_ods_stg.mtl_consumption_transactions (
  TRANSACTION_ID,
  CONSUMPTION_RELEASE_ID,
  CONSUMPTION_PO_HEADER_ID,
  CONSUMPTION_PROCESSED_FLAG,
  REQUEST_ID,
  CREATED_BY,
  PROGRAM_APPLICATION_ID,
  CREATION_DATE,
  PROGRAM_ID,
  LAST_UPDATED_BY,
  PROGRAM_UPDATE_DATE,
  LAST_UPDATE_DATE,
  LAST_UPDATE_LOGIN,
  NET_QTY,
  BATCH_ID,
  RATE,
  RATE_TYPE,
  TAX_CODE_ID,
  TAX_RATE,
  ERROR_CODE,
  RECOVERABLE_TAX,
  NON_RECOVERABLE_TAX,
  TAX_RECOVERY_RATE,
  PARENT_TRANSACTION_ID,
  CHARGE_ACCOUNT_ID,
  VARIANCE_ACCOUNT_ID,
  GLOBAL_AGREEMENT_FLAG,
  NEED_BY_DATE,
  ERROR_EXPLANATION,
  SECONDARY_NET_QTY,
  BLANKET_PRICE,
  PO_DISTRIBUTION_ID,
  INTERFACE_DISTRIBUTION_REF,
  TRANSACTION_SOURCE_ID,
  INVENTORY_ITEM_ID,
  ACCRUAL_ACCOUNT_ID,
  ORGANIZATION_ID,
  OWNING_ORGANIZATION_ID,
  TRANSACTION_DATE,
  PO_LINE_ID,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    TRANSACTION_ID,
    CONSUMPTION_RELEASE_ID,
    CONSUMPTION_PO_HEADER_ID,
    CONSUMPTION_PROCESSED_FLAG,
    REQUEST_ID,
    CREATED_BY,
    PROGRAM_APPLICATION_ID,
    CREATION_DATE,
    PROGRAM_ID,
    LAST_UPDATED_BY,
    PROGRAM_UPDATE_DATE,
    LAST_UPDATE_DATE,
    LAST_UPDATE_LOGIN,
    NET_QTY,
    BATCH_ID,
    RATE,
    RATE_TYPE,
    TAX_CODE_ID,
    TAX_RATE,
    ERROR_CODE,
    RECOVERABLE_TAX,
    NON_RECOVERABLE_TAX,
    TAX_RECOVERY_RATE,
    PARENT_TRANSACTION_ID,
    CHARGE_ACCOUNT_ID,
    VARIANCE_ACCOUNT_ID,
    GLOBAL_AGREEMENT_FLAG,
    NEED_BY_DATE,
    ERROR_EXPLANATION,
    SECONDARY_NET_QTY,
    BLANKET_PRICE,
    PO_DISTRIBUTION_ID,
    INTERFACE_DISTRIBUTION_REF,
    TRANSACTION_SOURCE_ID,
    INVENTORY_ITEM_ID,
    ACCRUAL_ACCOUNT_ID,
    ORGANIZATION_ID,
    OWNING_ORGANIZATION_ID,
    TRANSACTION_DATE,
    PO_LINE_ID,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.mtl_consumption_transactions
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (TRANSACTION_ID, kca_seq_id) IN (
      SELECT
        TRANSACTION_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.mtl_consumption_transactions
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        TRANSACTION_ID
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_consumption_transactions'
      )
    )
);