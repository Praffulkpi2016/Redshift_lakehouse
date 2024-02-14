/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_AR_TRANSACTION
WHERE
  (COALESCE(INV_TRX_ID, 0), COALESCE(CUSTOMER_TRX_LINE_ID, 0), COALESCE(INV_GL_DIST_ID, 0), COALESCE(INV_PAY_SCHEDULE_ID, 0)) IN (
    SELECT
      COALESCE(TMP.INV_TRX_ID, 0) AS INV_TRX_ID,
      COALESCE(TMP.CUSTOMER_TRX_LINE_ID, 0) AS CUSTOMER_TRX_LINE_ID,
      COALESCE(TMP.INV_GL_DIST_ID, 0) AS INV_GL_DIST_ID,
      COALESCE(TMP.INV_PAY_SCHEDULE_ID, 0) AS INV_PAY_SCHEDULE_ID
    FROM gold_bec_dwh.FACT_AR_TRANSACTION AS dw, (
      SELECT
        RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID AS INV_TRX_ID,
        RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_LINE_ID,
        AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID AS INV_PAY_SCHEDULE_ID,
        RA_CUST_TRX_LINE_GL_DIST_ALL.CUST_TRX_LINE_GL_DIST_ID AS INV_GL_DIST_ID
      FROM silver_bec_ods.RA_CUSTOMER_TRX_ALL AS RA_CUSTOMER_TRX_ALL
      INNER JOIN silver_bec_ods.RA_CUSTOMER_TRX_LINES_ALL AS RA_CUSTOMER_TRX_LINES_ALL
        ON RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_ID = RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID
      INNER JOIN silver_bec_ods.RA_BATCH_SOURCES_ALL AS RA_BATCH_SOURCES_ALL
        ON RA_CUSTOMER_TRX_ALL.BATCH_SOURCE_ID = RA_BATCH_SOURCES_ALL.BATCH_SOURCE_ID
        AND RA_CUSTOMER_TRX_ALL.org_id = RA_BATCH_SOURCES_ALL.org_id
      INNER JOIN silver_bec_ods.AR_PAYMENT_SCHEDULES_ALL AS AR_PAYMENT_SCHEDULES_ALL
        ON RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID = AR_PAYMENT_SCHEDULES_ALL.CUSTOMER_TRX_ID
      INNER JOIN silver_bec_ods.RA_CUST_TRX_LINE_GL_DIST_ALL AS RA_CUST_TRX_LINE_GL_DIST_ALL
        ON RA_CUST_TRX_LINE_GL_DIST_ALL.CUSTOMER_TRX_ID = RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID
        AND RA_CUST_TRX_LINE_GL_DIST_ALL.CUSTOMER_TRX_LINE_ID = RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_LINE_ID
      LEFT OUTER JOIN silver_bec_ods.RA_CUST_TRX_TYPES_ALL AS RA_CUST_TRX_TYPES_ALL
        ON RA_CUST_TRX_TYPES_ALL.CUST_TRX_TYPE_ID = RA_CUSTOMER_TRX_ALL.CUST_TRX_TYPE_ID
        AND RA_CUST_TRX_TYPES_ALL.ORG_ID = RA_CUSTOMER_TRX_ALL.ORG_ID
      LEFT OUTER JOIN silver_bec_ods.AR_CASH_RECEIPTS_ALL AS AR_CASH_RECEIPTS_ALL
        ON AR_PAYMENT_SCHEDULES_ALL.CASH_RECEIPT_ID = AR_CASH_RECEIPTS_ALL.CASH_RECEIPT_ID
      LEFT OUTER JOIN (
        SELECT
          *
        FROM silver_bec_ods.GL_DAILY_RATES AS GL_DAILY_RATES
        WHERE
          conversion_type = 'Corporate' AND TO_CURRENCY() = 'USD'
      ) AS DCR
        ON DCR.from_currency = AR_PAYMENT_SCHEDULES_ALL.invoice_currency_code
        AND DCR.conversion_date = AR_PAYMENT_SCHEDULES_ALL.GL_DATE
      LEFT OUTER JOIN silver_bec_ods.RA_RULES AS RA_RULES
        ON ra_rules.RULE_ID = RA_CUSTOMER_TRX_LINES_ALL.ACCOUNTING_RULE_ID
      WHERE
        1 = 1
        AND RA_CUSTOMER_TRX_ALL.COMPLETE_FLAG = 'Y'
        AND AR_PAYMENT_SCHEDULES_ALL.CLASS <> 'PMT'
        AND AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID <> -1
        AND (
          RA_CUST_TRX_LINE_GL_DIST_ALL.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_ar_transaction' AND batch_name = 'ar'
          )
          OR AR_PAYMENT_SCHEDULES_ALL.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_ar_transaction' AND batch_name = 'ar'
          )
          OR RA_CUSTOMER_TRX_ALL.is_deleted_flg = 'Y'
          OR RA_CUSTOMER_TRX_LINES_ALL.is_deleted_flg = 'Y'
          OR RA_BATCH_SOURCES_ALL.is_deleted_flg = 'Y'
          OR AR_PAYMENT_SCHEDULES_ALL.is_deleted_flg = 'Y'
          OR RA_CUST_TRX_LINE_GL_DIST_ALL.is_deleted_flg = 'Y'
          OR RA_CUST_TRX_TYPES_ALL.is_deleted_flg = 'Y'
          OR AR_CASH_RECEIPTS_ALL.is_deleted_flg = 'Y'
          OR DCR.is_deleted_flg = 'Y'
          OR RA_RULES.is_deleted_flg = 'Y'
        )
    ) AS TMP
    WHERE
      1 = 1
      AND DW.DW_LOAD_ID = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(TMP.INV_TRX_ID, 0) || '-' || COALESCE(TMP.CUSTOMER_TRX_LINE_ID, 0) || '-' || COALESCE(TMP.INV_GL_DIST_ID, 0) || '-' || COALESCE(TMP.INV_PAY_SCHEDULE_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_AR_TRANSACTION (
  invoice_info,
  inv_trx_id,
  inv_trx_id_key,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  inv_trx_number,
  customer_trx_line_id,
  inv_trx_date,
  inv_class,
  inv_bill_to_contact_id,
  inv_sold_to_site_use_id,
  inv_bill_to_customer_id_key,
  inv_bill_to_customer_id,
  inv_bill_to_site_id_key,
  inv_bill_to_site_use_id_key,
  inv_ship_to_site_use_id_key,
  inv_bill_site_use_id,
  inv_ship_site_use_id,
  inv_term_id,
  inv_term_id_key,
  inv_sales_rep_id,
  inv_sales_rep_id_key,
  inv_po,
  inv_territory,
  inv_currency_code,
  receipt_method_id,
  interface_header_attribute1,
  interface_header_context,
  inv_pay_site_use_id,
  inv_pay_schedule_id,
  inv_pay_schedule_id_key,
  inv_due_date,
  inv_amount,
  inv_remaining_amount,
  inv_cust_site_use_id,
  terms_sequence_number,
  inv_actual_date_closed,
  number_of_due_dates,
  inv_status,
  invoice_dist_info,
  inv_gl_dist_id,
  inv_gl_dist_id_key,
  inv_cc_id,
  inv_cc_id_key,
  inv_ledger_id,
  inv_ledger_id_key,
  inv_dist_amount,
  inv_gl_date,
  inv_gl_post_date,
  inv_acct_amount,
  inv_trx_type_id,
  inv_org_id,
  inv_org_id_key,
  ORG_ID,
  inv_legal_entity_id,
  inv_legal_entity_id_key,
  inv_dist_acct_class_code,
  inv_dist_latest_rec_flag,
  proj_num,
  batch_source_type,
  inventory_item_id,
  organization_id,
  invoice_currency_code,
  invoicing_rule_id,
  customer_reference,
  receipt_status,
  quantity_ordered,
  unit_selling_price,
  uom_code,
  revenue_amount,
  inventory_item_id_key,
  amount_applied,
  GBL_AMOUNT_APPLIED,
  GBL_INVOICE_AMOUNT,
  GBL_LINE_AMOUNT,
  GBL_DIST_AMOUNT,
  exchange_rate_type,
  exchange_rate,
  exchange_rate_date,
  accounting_rule,
  quantity_invoiced,
  line_number,
  IS_DELETED_FLG,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    'INVOICE_INFO' AS Invoice_Info,
    RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID AS INV_TRX_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID AS INV_TRX_ID_KEY,
    AR_PAYMENT_SCHEDULES_ALL.LAST_UPDATE_DATE,
    RA_CUSTOMER_TRX_ALL.LAST_UPDATED_BY,
    RA_CUSTOMER_TRX_ALL.CREATION_DATE,
    RA_CUSTOMER_TRX_ALL.CREATED_BY,
    RA_CUSTOMER_TRX_ALL.TRX_NUMBER AS INV_TRX_NUMBER,
    RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_LINE_ID,
    RA_CUSTOMER_TRX_ALL.TRX_DATE AS INV_TRX_DATE,
    AR_PAYMENT_SCHEDULES_ALL.CLASS AS INV_CLASS,
    RA_CUSTOMER_TRX_ALL.BILL_TO_CONTACT_ID AS INV_BILL_TO_CONTACT_ID,
    RA_CUSTOMER_TRX_ALL.SOLD_TO_SITE_USE_ID AS INV_SOLD_TO_SITE_USE_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL.BILL_TO_CUSTOMER_ID AS INV_BILL_TO_CUSTOMER_ID_KEY,
    RA_CUSTOMER_TRX_ALL.BILL_TO_CUSTOMER_ID AS INV_BILL_TO_CUSTOMER_ID,
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
        site_use_id = RA_CUSTOMER_TRX_ALL.BILL_TO_site_use_id
    ) AS INV_BILL_TO_SITE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL.BILL_TO_site_use_id AS INV_BILL_TO_SITE_USE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL.SHIP_TO_SITE_USE_ID AS INV_SHIP_TO_SITE_USE_ID_KEY,
    RA_CUSTOMER_TRX_ALL.BILL_TO_SITE_USE_ID AS INV_BILL_SITE_USE_ID,
    RA_CUSTOMER_TRX_ALL.SHIP_TO_SITE_USE_ID AS INV_SHIP_SITE_USE_ID,
    RA_CUSTOMER_TRX_ALL.TERM_ID AS INV_TERM_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL.TERM_ID AS INV_TERM_ID_KEY,
    RA_CUSTOMER_TRX_ALL.PRIMARY_SALESREP_ID AS INV_SALES_REP_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL.PRIMARY_SALESREP_ID AS INV_SALES_REP_ID_KEY,
    RA_CUSTOMER_TRX_ALL.PURCHASE_ORDER AS INV_PO,
    RA_CUSTOMER_TRX_ALL.TERRITORY_ID AS INV_TERRITORY,
    RA_CUSTOMER_TRX_ALL.INVOICE_CURRENCY_CODE AS INV_CURRENCY_CODE,
    RA_CUSTOMER_TRX_ALL.RECEIPT_METHOD_ID,
    RA_CUSTOMER_TRX_ALL.INTERFACE_HEADER_ATTRIBUTE1,
    RA_CUSTOMER_TRX_ALL.INTERFACE_HEADER_CONTEXT,
    RA_CUSTOMER_TRX_ALL.PAYING_SITE_USE_ID AS INV_PAY_SITE_USE_ID,
    AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID AS INV_PAY_SCHEDULE_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID AS INV_PAY_SCHEDULE_ID_KEY,
    AR_PAYMENT_SCHEDULES_ALL.DUE_DATE AS INV_DUE_DATE,
    AR_PAYMENT_SCHEDULES_ALL.AMOUNT_DUE_ORIGINAL AS INV_AMOUNT,
    AR_PAYMENT_SCHEDULES_ALL.AMOUNT_DUE_REMAINING AS INV_REMAINING_AMOUNT,
    AR_PAYMENT_SCHEDULES_ALL.CUSTOMER_SITE_USE_ID AS INV_CUST_SITE_USE_ID,
    AR_PAYMENT_SCHEDULES_ALL.TERMS_SEQUENCE_NUMBER,
    AR_PAYMENT_SCHEDULES_ALL.ACTUAL_DATE_CLOSED AS INV_ACTUAL_DATE_CLOSED,
    AR_PAYMENT_SCHEDULES_ALL.NUMBER_OF_DUE_DATES,
    AR_PAYMENT_SCHEDULES_ALL.STATUS AS INV_STATUS,
    'INV_DIST_INFO' AS Invoice_Dist_Info,
    RA_CUST_TRX_LINE_GL_DIST_ALL.CUST_TRX_LINE_GL_DIST_ID AS INV_GL_DIST_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUST_TRX_LINE_GL_DIST_ALL.CUST_TRX_LINE_GL_DIST_ID AS INV_GL_DIST_ID_KEY,
    RA_CUST_TRX_LINE_GL_DIST_ALL.CODE_COMBINATION_ID AS INV_CC_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUST_TRX_LINE_GL_DIST_ALL.CODE_COMBINATION_ID AS INV_CC_ID_KEY,
    RA_CUST_TRX_LINE_GL_DIST_ALL.SET_OF_BOOKS_ID AS INV_LEDGER_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUST_TRX_LINE_GL_DIST_ALL.SET_OF_BOOKS_ID AS INV_LEDGER_ID_KEY,
    RA_CUST_TRX_LINE_GL_DIST_ALL.AMOUNT AS INV_DIST_AMOUNT,
    RA_CUST_TRX_LINE_GL_DIST_ALL.GL_DATE AS INV_GL_DATE,
    RA_CUST_TRX_LINE_GL_DIST_ALL.GL_POSTED_DATE AS INV_GL_POST_DATE,
    RA_CUST_TRX_LINE_GL_DIST_ALL.ACCTD_AMOUNT AS INV_ACCT_AMOUNT,
    RA_CUST_TRX_TYPES_ALL.CUST_TRX_TYPE_ID AS INV_TRX_TYPE_ID,
    RA_CUSTOMER_TRX_ALL.ORG_ID AS INV_ORG_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL.ORG_ID AS INV_ORG_ID_KEY,
    RA_CUSTOMER_TRX_ALL.ORG_ID AS ORG_ID,
    RA_CUSTOMER_TRX_ALL.LEGAL_ENTITY_ID AS INV_LEGAL_ENTITY_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_ALL.LEGAL_ENTITY_ID AS INV_LEGAL_ENTITY_ID_KEY,
    RA_CUST_TRX_LINE_GL_DIST_ALL.ACCOUNT_CLASS AS INV_DIST_ACCT_CLASS_CODE,
    RA_CUST_TRX_LINE_GL_DIST_ALL.LATEST_REC_FLAG AS INV_DIST_LATEST_REC_FLAG,
    RA_CUSTOMER_TRX_ALL.INTERFACE_HEADER_ATTRIBUTE1 AS PROJ_NUM,
    RA_BATCH_SOURCES_ALL.BATCH_SOURCE_TYPE,
    RA_CUSTOMER_TRX_LINES_ALL.INVENTORY_ITEM_ID,
    RA_CUSTOMER_TRX_LINES_ALL.WAREHOUSE_ID AS ORGANIZATION_ID,
    RA_CUSTOMER_TRX_ALL.INVOICE_CURRENCY_CODE,
    RA_CUSTOMER_TRX_ALL.INVOICING_RULE_ID,
    RA_CUSTOMER_TRX_ALL.CUSTOMER_REFERENCE,
    RA_CUSTOMER_TRX_ALL.STATUS_TRX AS Receipt_Status,
    RA_CUSTOMER_TRX_LINES_ALL.QUANTITY_ORDERED,
    RA_CUSTOMER_TRX_LINES_ALL.UNIT_SELLING_PRICE,
    RA_CUSTOMER_TRX_LINES_ALL.UOM_CODE,
    RA_CUSTOMER_TRX_LINES_ALL.REVENUE_AMOUNT,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RA_CUSTOMER_TRX_LINES_ALL.INVENTORY_ITEM_ID AS INVENTORY_ITEM_ID_KEY,
    AR_PAYMENT_SCHEDULES_ALL.AMOUNT_APPLIED,
    CAST(COALESCE(AR_PAYMENT_SCHEDULES_ALL.AMOUNT_APPLIED, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_APPLIED,
    CAST(COALESCE(AR_PAYMENT_SCHEDULES_ALL.AMOUNT_DUE_ORIGINAL, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_INVOICE_AMOUNT,
    CAST(COALESCE(RA_CUSTOMER_TRX_LINES_ALL.REVENUE_AMOUNT, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_LINE_AMOUNT,
    CAST(COALESCE(RA_CUST_TRX_LINE_GL_DIST_ALL.AMOUNT, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_DIST_AMOUNT,
    ar_cash_Receipts_all.EXCHANGE_RATE_TYPE,
    ar_cash_Receipts_all.EXCHANGE_RATE,
    ar_cash_Receipts_all.EXCHANGE_DATE AS EXCHANGE_RATE_DATE,
    RA_RULES.name AS ACCOUNTING_RULE,
    RA_CUSTOMER_TRX_LINES_ALL.quantity_invoiced,
    RA_CUSTOMER_TRX_LINES_ALL.line_number,
    'N' AS IS_DELETED_FLG,
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
    ) || '-' || COALESCE(RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID, 0) || '-' || COALESCE(RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_LINE_ID, 0) || '-' || COALESCE(RA_CUST_TRX_LINE_GL_DIST_ALL.CUST_TRX_LINE_GL_DIST_ID, 0) || '-' || COALESCE(AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.RA_CUSTOMER_TRX_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RA_CUSTOMER_TRX_ALL
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RA_CUSTOMER_TRX_LINES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RA_CUSTOMER_TRX_LINES_ALL
    ON RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_ID = RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RA_BATCH_SOURCES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RA_BATCH_SOURCES_ALL
    ON RA_CUSTOMER_TRX_ALL.BATCH_SOURCE_ID = RA_BATCH_SOURCES_ALL.BATCH_SOURCE_ID
    AND RA_CUSTOMER_TRX_ALL.org_id = RA_BATCH_SOURCES_ALL.org_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.AR_PAYMENT_SCHEDULES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AR_PAYMENT_SCHEDULES_ALL
    ON RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID = AR_PAYMENT_SCHEDULES_ALL.CUSTOMER_TRX_ID
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RA_CUST_TRX_LINE_GL_DIST_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RA_CUST_TRX_LINE_GL_DIST_ALL
    ON RA_CUST_TRX_LINE_GL_DIST_ALL.CUSTOMER_TRX_ID = RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID
    AND RA_CUST_TRX_LINE_GL_DIST_ALL.CUSTOMER_TRX_LINE_ID = RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_LINE_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RA_CUST_TRX_TYPES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RA_CUST_TRX_TYPES_ALL
    ON RA_CUST_TRX_TYPES_ALL.CUST_TRX_TYPE_ID = RA_CUSTOMER_TRX_ALL.CUST_TRX_TYPE_ID
    AND RA_CUST_TRX_TYPES_ALL.ORG_ID = RA_CUSTOMER_TRX_ALL.ORG_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.AR_CASH_RECEIPTS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AR_CASH_RECEIPTS_ALL
    ON AR_PAYMENT_SCHEDULES_ALL.CASH_RECEIPT_ID = AR_CASH_RECEIPTS_ALL.CASH_RECEIPT_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM (
      SELECT
        *
      FROM silver_bec_ods.GL_DAILY_RATES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS GL_DAILY_RATES
    WHERE
      conversion_type = 'Corporate' AND TO_CURRENCY() = 'USD'
  ) AS DCR
    ON DCR.from_currency = AR_PAYMENT_SCHEDULES_ALL.invoice_currency_code
    AND DCR.conversion_date = AR_PAYMENT_SCHEDULES_ALL.GL_DATE
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RA_RULES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RA_RULES
    ON ra_rules.RULE_ID = RA_CUSTOMER_TRX_LINES_ALL.ACCOUNTING_RULE_ID
  WHERE
    1 = 1
    AND RA_CUSTOMER_TRX_ALL.COMPLETE_FLAG = 'Y'
    AND AR_PAYMENT_SCHEDULES_ALL.CLASS <> 'PMT'
    AND AR_PAYMENT_SCHEDULES_ALL.PAYMENT_SCHEDULE_ID <> -1
    AND (
      RA_CUST_TRX_LINE_GL_DIST_ALL.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_ar_transaction' AND batch_name = 'ar'
      )
      OR AR_PAYMENT_SCHEDULES_ALL.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_ar_transaction' AND batch_name = 'ar'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ar_transaction' AND batch_name = 'ar';