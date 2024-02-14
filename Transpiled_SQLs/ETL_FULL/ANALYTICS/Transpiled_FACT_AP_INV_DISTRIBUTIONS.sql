DROP table IF EXISTS gold_bec_dwh.FACT_AP_INV_DISTRIBUTIONS;
CREATE TABLE gold_bec_dwh.FACT_AP_INV_DISTRIBUTIONS AS
(
  SELECT
    AID.ACCOUNTING_DATE,
    AID.ASSETS_ADDITION_FLAG,
    AID.ASSETS_TRACKING_FLAG,
    AID.DISTRIBUTION_LINE_NUMBER,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AID.DIST_CODE_COMBINATION_ID AS GL_ACCOUNT_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AID.invoice_id AS INVOICE_ID_KEY,
    AID.INVOICE_ID,
    AID.LINE_TYPE_LOOKUP_CODE,
    AID.PERIOD_NAME,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AID.SET_OF_BOOKS_ID AS LEDGER_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIA.VENDOR_ID AS VENDOR_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIA.VENDOR_SITE_ID AS VENDOR_SITE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIA.TERMS_ID AS TERMS_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(AIA.BATCH_ID, 0) AS BATCH_ID_KEY,
    AID.AMOUNT,
    AID.BASE_AMOUNT,
    AID.DESCRIPTION,
    AID.FINAL_MATCH_FLAG,
    AID.POSTED_FLAG,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AID.PO_DISTRIBUTION_ID AS PO_DISTRIBUTION_ID_KEY,
    AID.QUANTITY_INVOICED,
    AID.REVERSAL_FLAG,
    AID.UNIT_PRICE,
    AID.EXPENDITURE_ITEM_DATE,
    AID.EXPENDITURE_ORGANIZATION_ID,
    AID.EXPENDITURE_TYPE,
    AID.PARENT_INVOICE_ID,
    AID.PREPAY_AMOUNT_REMAINING,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AID.PROJECT_ID AS PROJECT_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AID.PROJECT_ID || '-' || AID.TASK_ID AS PROJECT_TASK_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AID.ORG_ID AS ORG_ID_KEY,
    AID.ORG_ID AS ORG_ID,
    AID.RECEIPT_VERIFIED_FLAG,
    AID.RECEIPT_REQUIRED_FLAG,
    AID.RECEIPT_MISSING_FLAG,
    AID.JUSTIFICATION,
    AID.EXPENSE_GROUP,
    AID.START_EXPENSE_DATE,
    AID.END_EXPENSE_DATE,
    AID.RECEIPT_CURRENCY_CODE,
    AID.RECEIPT_CONVERSION_RATE,
    AID.RECEIPT_CURRENCY_AMOUNT,
    AID.DAILY_AMOUNT,
    AID.ADJUSTMENT_REASON,
    AID.RCV_TRANSACTION_ID,
    AID.INVOICE_DISTRIBUTION_ID,
    AID.MERCHANT_DOCUMENT_NUMBER,
    AID.MERCHANT_NAME,
    AID.MERCHANT_REFERENCE,
    AID.MERCHANT_TAX_REG_NUMBER,
    AID.MERCHANT_TAXPAYER_ID,
    AID.COUNTRY_OF_SUPPLY,
    AID.ACCOUNTING_EVENT_ID,
    AID.CANCELLATION_FLAG,
    AID.INVOICE_LINE_NUMBER,
    AID.ASSET_BOOK_TYPE_CODE,
    AID.ASSET_CATEGORY_ID,
    AID.SUMMARY_TAX_LINE_ID,
    AID.TAX_CODE_ID,
    AID.TOTAL_DIST_AMOUNT,
    AID.TOTAL_DIST_BASE_AMOUNT,
    AID.CANCELLED_FLAG,
    AILA.PO_HEADER_ID,
    AILA.PO_LINE_ID,
    AILA.LINE_NUMBER,
    CAST(COALESCE(AID.TOTAL_DIST_AMOUNT, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_DIST_AMOUNT,
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
    ) || '-' || COALESCE(AID.INVOICE_ID, 0) /* ||' -'|| nvl(AILA.LINE_NUMBER,0) */ || '-' || COALESCE(AID.INVOICE_LINE_NUMBER, 0) || '-' || COALESCE(AID.INVOICE_DISTRIBUTION_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AID
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.AP_INVOICES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AIA
    ON AID.INVOICE_ID = AIA.INVOICE_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.AP_INVOICE_LINES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AILA
    ON AIA.INVOICE_ID = AILA.INVOICE_ID AND AID.INVOICE_LINE_NUMBER = AILA.LINE_NUMBER
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.GL_DAILY_RATES
    WHERE
      is_deleted_flg <> 'Y' AND to_currency = 'USD' AND conversion_type = 'Corporate'
  ) AS DCR
    ON AIA.invoice_currency_code = DCR.from_currency
    AND AIA.invoice_date = DCR.conversion_date
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ap_inv_distributions' AND batch_name = 'ap';