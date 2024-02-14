/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_AR_CREDIT_MEMO
WHERE
  (COALESCE(RECEIVABLE_APPLICATION_ID, 0), COALESCE(CM_DIST_LINE_ID, 0)) IN (
    SELECT
      COALESCE(ods.RECEIVABLE_APPLICATION_ID, 0) AS RECEIVABLE_APPLICATION_ID,
      COALESCE(ods.CM_DIST_LINE_ID, 0) AS CM_DIST_LINE_ID
    FROM gold_bec_dwh.FACT_AR_CREDIT_MEMO AS dw, (
      SELECT
        AR_RECEIVABLE_APPLICATIONS_ALL.RECEIVABLE_APPLICATION_ID,
        AR_DISTRIBUTIONS_ALL.LINE_ID AS CM_DIST_LINE_ID
      FROM silver_bec_ods.AR_RECEIVABLE_APPLICATIONS_ALL AS AR_RECEIVABLE_APPLICATIONS_ALL
      INNER JOIN silver_bec_ods.AR_DISTRIBUTIONS_ALL AS AR_DISTRIBUTIONS_ALL
        ON AR_DISTRIBUTIONS_ALL.SOURCE_ID = AR_RECEIVABLE_APPLICATIONS_ALL.RECEIVABLE_APPLICATION_ID
      WHERE
        AR_RECEIVABLE_APPLICATIONS_ALL.APPLICATION_TYPE = 'CM'
        AND COALESCE(AR_RECEIVABLE_APPLICATIONS_ALL.CONFIRMED_FLAG, 'Y') = 'Y'
        AND AR_DISTRIBUTIONS_ALL.SOURCE_TABLE = 'RA'
        AND AR_DISTRIBUTIONS_ALL.SOURCE_TYPE = 'REC'
        AND (
          AR_RECEIVABLE_APPLICATIONS_ALL.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_ar_credit_memo' AND batch_name = 'ar'
          )
          OR AR_RECEIVABLE_APPLICATIONS_ALL.is_deleted_flg = 'Y'
          OR AR_DISTRIBUTIONS_ALL.is_deleted_flg = 'Y'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.RECEIVABLE_APPLICATION_ID, 0) || '-' || COALESCE(ods.CM_DIST_LINE_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_AR_CREDIT_MEMO (
  receipt_app,
  receivable_application_id,
  last_update_date,
  cm_applied_amt,
  cm_applied_gl_date,
  ledger_id,
  cm_apply_date,
  cm_applied_post_date,
  org_id_key,
  org_id,
  credit_memo_info,
  cm_trx_id,
  cm_trx_number,
  cm_type_id_key,
  cm_type_id,
  cm_trx_date,
  cm_bill_to_cust_id,
  cm_bill_site_use_id,
  cm_ship_site_use_id,
  cm_currency_code,
  receipt_method_id,
  cm_amount,
  payment_schedule_id,
  cm_due_date,
  cm_status,
  invoice_info,
  inv_trx_id,
  inv_trx_number,
  invoice_amount,
  inv_type_id,
  inv_trx_date,
  inv_bill_to_contact_id_key,
  inv_sold_to_site_use_id_key,
  inv_bill_to_customer_id_key,
  inv_bill_site_use_id_key,
  inv_ship_site_use_id_key,
  inv_term_id_key,
  inv_sales_rep_id_key,
  inv_sales_rep_id,
  inv_term_id,
  inv_ship_to_customer_id,
  inv_po,
  inv_territory,
  interface_header_attribute1,
  interface_header_context,
  inv_pay_site_use_id,
  inv_pay_schedule_id,
  inv_due_date,
  inv_status,
  inv_cust_site_use_id_key,
  terms_sequence_number,
  distribution_info,
  cm_dist_line_id,
  cm_dist_cc_id_key,
  cm_amt_debit,
  cm_amt_credit,
  cm_acct_amt_debit,
  cm_acct_amt_credit,
  cm_legal_entity_id,
  inv_legal_entity_id,
  proj_num,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    'RECEIPT_APP' AS Receipt_App,
    AR_RECEIVABLE_APPLICATIONS_ALL.RECEIVABLE_APPLICATION_ID,
    AR_RECEIVABLE_APPLICATIONS_ALL.LAST_UPDATE_DATE,
    AR_RECEIVABLE_APPLICATIONS_ALL.AMOUNT_APPLIED AS CM_Applied_Amt,
    AR_RECEIVABLE_APPLICATIONS_ALL.GL_DATE AS CM_Applied_GL_Date,
    AR_RECEIVABLE_APPLICATIONS_ALL.SET_OF_BOOKS_ID AS Ledger_ID,
    AR_RECEIVABLE_APPLICATIONS_ALL.APPLY_DATE AS CM_Apply_Date,
    AR_RECEIVABLE_APPLICATIONS_ALL.GL_POSTED_DATE AS CM_Applied_Post_Date,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AR_RECEIVABLE_APPLICATIONS_ALL.ORG_ID AS ORG_ID_KEY,
    AR_RECEIVABLE_APPLICATIONS_ALL.ORG_ID AS ORG_ID,
    'CREDIT_MEMO_INFO' AS Credit_Memo_Info,
    RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID AS CM_Trx_Id,
    RA_CUSTOMER_TRX_ALL.TRX_NUMBER AS CM_Trx_Number,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL.CUST_TRX_TYPE_ID AS CM_TYPE_ID_KEY,
    RA_CUSTOMER_TRX_ALL.CUST_TRX_TYPE_ID AS CM_TYPE_ID,
    RA_CUSTOMER_TRX_ALL.TRX_DATE AS CM_Trx_Date,
    RA_CUSTOMER_TRX_ALL.BILL_TO_CUSTOMER_ID AS CM_Bill_To_Cust_Id,
    RA_CUSTOMER_TRX_ALL.BILL_TO_SITE_USE_ID AS CM_Bill_Site_Use_Id,
    RA_CUSTOMER_TRX_ALL1.SHIP_TO_SITE_USE_ID AS CM_SHIP_SITE_USE_ID,
    RA_CUSTOMER_TRX_ALL.INVOICE_CURRENCY_CODE AS CM_Currency_Code,
    RA_CUSTOMER_TRX_ALL.RECEIPT_METHOD_ID,
    AR_PAYMENT_SCHEDULES_ALL1.AMOUNT_DUE_ORIGINAL AS CM_Amount,
    AR_PAYMENT_SCHEDULES_ALL1.PAYMENT_SCHEDULE_ID,
    AR_PAYMENT_SCHEDULES_ALL1.DUE_DATE AS CM_Due_Date,
    AR_PAYMENT_SCHEDULES_ALL1.STATUS AS CM_Status,
    'INVOICE_INFO' AS Invoice_Info,
    RA_CUSTOMER_TRX_ALL1.CUSTOMER_TRX_ID AS Inv_Trx_Id,
    RA_CUSTOMER_TRX_ALL1.TRX_NUMBER AS Inv_Trx_number,
    AR_PAYMENT_SCHEDULES_ALL.AMOUNT_DUE_ORIGINAL AS Invoice_Amount,
    RA_CUSTOMER_TRX_ALL1.CUST_TRX_TYPE_ID AS Inv_Type_Id,
    RA_CUSTOMER_TRX_ALL1.TRX_DATE AS Inv_Trx_Date,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL1.BILL_TO_CONTACT_ID AS Inv_Bill_To_Contact_Id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || (
      SELECT
        CUST_ACCT_SITE_ID
      FROM (
        SELECT
          *
        FROM silver_bec_ods.HZ_CUST_SITE_USES_ALL
        WHERE
          is_deleted_flg <> 'Y'
      ) AS HZ_CUST_SITE_USES_ALL
      WHERE
        site_use_id = RA_CUSTOMER_TRX_ALL1.SOLD_TO_SITE_USE_ID
    ) || '-' || RA_CUSTOMER_TRX_ALL1.SOLD_TO_SITE_USE_ID AS INV_SOLD_TO_SITE_USE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL1.BILL_TO_CUSTOMER_ID AS INV_BILL_TO_CUSTOMER_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL1.BILL_TO_SITE_USE_ID AS INV_BILL_SITE_USE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL1.SHIP_TO_SITE_USE_ID AS INV_SHIP_SITE_USE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL1.TERM_ID AS INV_TERM_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL1.PRIMARY_SALESREP_ID AS INV_SALES_REP_ID_KEY,
    RA_CUSTOMER_TRX_ALL1.PRIMARY_SALESREP_ID AS INV_SALES_REP_ID,
    RA_CUSTOMER_TRX_ALL1.TERM_ID AS INV_TERM_ID,
    RA_CUSTOMER_TRX_ALL1.SHIP_TO_CUSTOMER_ID AS INV_SHIP_TO_CUSTOMER_ID,
    RA_CUSTOMER_TRX_ALL1.PURCHASE_ORDER AS Inv_PO,
    RA_CUSTOMER_TRX_ALL1.TERRITORY_ID AS Inv_Territory,
    RA_CUSTOMER_TRX_ALL1.INTERFACE_HEADER_ATTRIBUTE1,
    RA_CUSTOMER_TRX_ALL1.INTERFACE_HEADER_CONTEXT,
    RA_CUSTOMER_TRX_ALL1.PAYING_SITE_USE_ID AS Inv_Pay_Site_Use_Id,
    AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID AS Inv_Pay_Schedule_Id,
    AR_PAYMENT_SCHEDULES_ALL.DUE_DATE AS Inv_Due_Date,
    AR_PAYMENT_SCHEDULES_ALL.STATUS AS Inv_Status,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AR_PAYMENT_SCHEDULES_ALL.CUSTOMER_SITE_USE_ID AS INV_CUST_SITE_USE_ID_KEY,
    AR_PAYMENT_SCHEDULES_ALL.TERMS_SEQUENCE_NUMBER,
    'DISTRIBUTION_INFO' AS Distribution_Info,
    AR_DISTRIBUTIONS_ALL.LINE_ID AS CM_DIST_LINE_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AR_DISTRIBUTIONS_ALL.CODE_COMBINATION_ID AS CM_DIST_CC_ID_KEY,
    AR_DISTRIBUTIONS_ALL.AMOUNT_DR AS CM_AMT_Debit,
    AR_DISTRIBUTIONS_ALL.AMOUNT_CR AS CM_AMT_Credit,
    AR_DISTRIBUTIONS_ALL.ACCTD_AMOUNT_DR AS CM_Acct_AMT_Debit,
    AR_DISTRIBUTIONS_ALL.ACCTD_AMOUNT_CR AS CM_Acct_AMT_Credit,
    RA_CUSTOMER_TRX_ALL.LEGAL_ENTITY_ID AS CM_Legal_Entity_Id,
    RA_CUSTOMER_TRX_ALL1.LEGAL_ENTITY_ID AS Inv_Legal_Entity_Id,
    RA_CUSTOMER_TRX_ALL.INTERFACE_HEADER_ATTRIBUTE1 AS PROJ_NUM,
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
    ) || '-' || COALESCE(AR_RECEIVABLE_APPLICATIONS_ALL.RECEIVABLE_APPLICATION_ID, 0) || '-' || COALESCE(AR_DISTRIBUTIONS_ALL.LINE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.AR_RECEIVABLE_APPLICATIONS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AR_RECEIVABLE_APPLICATIONS_ALL
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.AR_DISTRIBUTIONS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AR_DISTRIBUTIONS_ALL
    ON AR_DISTRIBUTIONS_ALL.SOURCE_ID = AR_RECEIVABLE_APPLICATIONS_ALL.RECEIVABLE_APPLICATION_ID
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RA_CUSTOMER_TRX_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RA_CUSTOMER_TRX_ALL
    ON RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID = AR_RECEIVABLE_APPLICATIONS_ALL.CUSTOMER_TRX_ID
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.AR_PAYMENT_SCHEDULES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AR_PAYMENT_SCHEDULES_ALL1
    ON AR_PAYMENT_SCHEDULES_ALL1.PAYMENT_SCHEDULE_ID = AR_RECEIVABLE_APPLICATIONS_ALL.PAYMENT_SCHEDULE_ID
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RA_CUSTOMER_TRX_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RA_CUSTOMER_TRX_ALL1
    ON RA_CUSTOMER_TRX_ALL1.CUSTOMER_TRX_ID = AR_RECEIVABLE_APPLICATIONS_ALL.APPLIED_CUSTOMER_TRX_ID
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.AR_PAYMENT_SCHEDULES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AR_PAYMENT_SCHEDULES_ALL
    ON AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID = AR_RECEIVABLE_APPLICATIONS_ALL.APPLIED_PAYMENT_SCHEDULE_ID
  WHERE
    AR_RECEIVABLE_APPLICATIONS_ALL.APPLICATION_TYPE = 'CM'
    AND COALESCE(AR_RECEIVABLE_APPLICATIONS_ALL.CONFIRMED_FLAG, 'Y') = 'Y'
    AND AR_DISTRIBUTIONS_ALL.SOURCE_TABLE = 'RA'
    AND AR_DISTRIBUTIONS_ALL.SOURCE_TYPE = 'REC'
    AND (
      AR_RECEIVABLE_APPLICATIONS_ALL.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_ar_credit_memo' AND batch_name = 'ar'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ar_credit_memo' AND batch_name = 'ar';