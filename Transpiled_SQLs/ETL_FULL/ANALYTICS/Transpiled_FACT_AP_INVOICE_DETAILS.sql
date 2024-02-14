DROP table IF EXISTS gold_bec_dwh.fact_ap_invoice_details;
CREATE TABLE gold_bec_dwh.fact_ap_invoice_details AS
(
  SELECT
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIA.INVOICE_ID AS INVOICE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIA.VENDOR_ID AS VENDOR_ID_KEY,
    AIA.INVOICE_NUM,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIA.SET_OF_BOOKS_ID AS LEDGER_ID_KEY,
    AIA.INVOICE_CURRENCY_CODE,
    AIA.PAYMENT_CURRENCY_CODE,
    AIA.INVOICE_AMOUNT,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIA.VENDOR_SITE_ID AS VENDOR_SITE_ID_KEY,
    AIA.AMOUNT_PAID,
    AIA.DISCOUNT_AMOUNT_TAKEN,
    AIA.INVOICE_DATE,
    AIA.SOURCE,
    AIA.INVOICE_TYPE_LOOKUP_CODE,
    AIA.DESCRIPTION,
    AIA.TAX_AMOUNT,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIA.TERMS_ID AS TERMS_ID_KEY,
    AIA.TERMS_DATE,
    AIA.PAYMENT_STATUS_FLAG,
    AIA.PO_HEADER_ID,
    AIA.FREIGHT_AMOUNT,
    AIA.INVOICE_ID,
    AIA.INVOICE_RECEIVED_DATE,
    AIA.EXCHANGE_RATE,
    AIA.EXCHANGE_RATE_TYPE,
    AIA.EXCHANGE_DATE,
    AIA.CANCELLED_DATE,
    AIA.CREATION_DATE,
    AIA.LAST_UPDATE_DATE,
    AIA.CANCELLED_AMOUNT,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIA.PROJECT_ID AS PROJECT_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIA.PROJECT_ID || '-' || AIA.TASK_ID AS PROJECT_TASK_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AIA.ORG_ID AS ORG_ID_KEY,
    AIA.ORG_ID AS ORG_ID,
    AIA.PAY_CURR_INVOICE_AMOUNT,
    AIA.GL_DATE,
    AIA.TOTAL_TAX_AMOUNT,
    AIA.LEGAL_ENTITY_ID,
    AIA.PARTY_ID,
    AIA.PARTY_SITE_ID,
    AIA.REMIT_TO_SUPPLIER_NAME,
    AIA.REMIT_TO_SUPPLIER_SITE,
    AIA.AMOUNT_APPLICABLE_TO_DISCOUNT,
    AIA.PAYMENT_METHOD_LOOKUP_CODE,
    AIA.PAY_GROUP_LOOKUP_CODE,
    AIA.ACCTS_PAY_CODE_COMBINATION_ID,
    AIA.BASE_AMOUNT AS `INV_AMT_FUNC_CURRENCY`,
    AIA.ORIGINAL_PREPAYMENT_AMOUNT,
    AIA.PREPAY_FLAG,
    AIA.EXPENDITURE_TYPE,
    AIA.PAYMENT_AMOUNT_TOTAL,
    AILA.LINE_NUMBER,
    AILA.LINE_TYPE_LOOKUP_CODE,
    AILA.DESCRIPTION AS `LINE_DESCRIPTION`,
    AILA.LINE_SOURCE,
    AILA.ORG_ID AS `LINE_ORG_ID`,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || AILA.INVENTORY_ITEM_ID AS INVENTORY_ITEM_ID_KEY,
    AILA.ITEM_DESCRIPTION,
    AILA.MATCH_TYPE,
    AILA.DEFAULT_DIST_CCID,
    AILA.ACCOUNTING_DATE,
    AILA.PERIOD_NAME,
    AILA.AMOUNT,
    AILA.BASE_AMOUNT AS `INVLIN_AMT_FUNC_CURRENCY`,
    AILA.ROUNDING_AMT,
    AILA.QUANTITY_INVOICED,
    AILA.UNIT_MEAS_LOOKUP_CODE,
    AILA.UNIT_PRICE,
    AILA.CANCELLED_FLAG,
    AILA.INCOME_TAX_REGION,
    AILA.PREPAY_INVOICE_ID,
    AILA.PREPAY_LINE_NUMBER,
    AILA.PO_HEADER_ID AS LINE_PO_HEADER,
    AILA.PO_LINE_ID,
    AILA.PO_RELEASE_ID,
    AILA.PO_LINE_LOCATION_ID,
    AILA.PO_DISTRIBUTION_ID,
    AILA.RCV_TRANSACTION_ID,
    AILA.ASSETS_TRACKING_FLAG,
    AILA.ASSET_BOOK_TYPE_CODE,
    AILA.ASSET_CATEGORY_ID,
    AILA.EXPENDITURE_TYPE AS LINE_EXPENDITURE_TYPE,
    AILA.TAX_REGIME_CODE,
    AILA.TAX,
    AILA.TAX_JURISDICTION_CODE,
    AILA.TAX_STATUS_CODE,
    AILA.TAX_RATE_ID,
    AILA.TAX_RATE_CODE,
    AILA.TAX_RATE,
    AILA.TAX_CODE_ID,
    AILA.TAX_CLASSIFICATION_CODE,
    AILA.RCV_SHIPMENT_LINE_ID,
    AILA.WFAPPROVAL_STATUS,
    CAST(COALESCE(AIA.invoice_amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_INVOICE_AMOUNT,
    CAST(COALESCE(AIA.amount_paid, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_PAID,
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
    ) || '-' || COALESCE(AILA.INVOICE_ID, 0) || '-' || COALESCE(AILA.LINE_NUMBER, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.AP_INVOICE_LINES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AILA, (
    SELECT
      *
    FROM silver_bec_ods.AP_INVOICES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS AIA, (
    SELECT
      *
    FROM silver_bec_ods.GL_DAILY_RATES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS DCR
  WHERE
    1 = 1
    AND AIA.INVOICE_ID = AILA.INVOICE_ID
    AND DCR.TO_CURRENCY() = 'USD'
    AND DCR.CONVERSION_TYPE() = 'Corporate'
    AND AIA.invoice_currency_code = DCR.FROM_CURRENCY()
    AND DCR.CONVERSION_DATE() = AIA.invoice_date
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ap_invoice_details' AND batch_name = 'ap';