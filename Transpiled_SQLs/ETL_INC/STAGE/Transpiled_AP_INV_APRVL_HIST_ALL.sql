TRUNCATE table bronze_bec_ods_stg.AP_INV_APRVL_HIST_ALL;
INSERT INTO bronze_bec_ods_stg.AP_INV_APRVL_HIST_ALL (
  APPROVAL_HISTORY_ID,
  INVOICE_ID,
  ITERATION,
  RESPONSE,
  APPROVER_ID,
  APPROVER_NAME,
  AMOUNT_APPROVED,
  APPROVER_COMMENTS,
  CREATED_BY,
  CREATION_DATE,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  ORG_ID,
  NOTIFICATION_ORDER,
  ORIG_SYSTEM,
  ITEM_CLASS,
  ITEM_ID,
  LINE_NUMBER,
  HOLD_ID,
  HISTORY_TYPE,
  `APPROVER_COMMENTS#1`,
  `LINE_NUMBER#1`,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    APPROVAL_HISTORY_ID,
    INVOICE_ID,
    ITERATION,
    RESPONSE,
    APPROVER_ID,
    APPROVER_NAME,
    AMOUNT_APPROVED,
    APPROVER_COMMENTS,
    CREATED_BY,
    CREATION_DATE,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    LAST_UPDATE_LOGIN,
    ORG_ID,
    NOTIFICATION_ORDER,
    ORIG_SYSTEM,
    ITEM_CLASS,
    ITEM_ID,
    LINE_NUMBER,
    HOLD_ID,
    HISTORY_TYPE,
    `APPROVER_COMMENTS#1`,
    `LINE_NUMBER#1`,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.AP_INV_APRVL_HIST_ALL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(APPROVAL_HISTORY_ID, '0'), kca_seq_id) IN (
      SELECT
        COALESCE(APPROVAL_HISTORY_ID, '0') AS APPROVAL_HISTORY_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.AP_INV_APRVL_HIST_ALL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(APPROVAL_HISTORY_ID, '0')
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'ap_inv_aprvl_hist_all'
    )
);