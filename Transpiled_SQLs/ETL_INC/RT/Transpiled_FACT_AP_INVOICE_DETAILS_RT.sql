TRUNCATE table gold_bec_dwh_rpt.FACT_AP_INVOICE_DETAILS_RT;
WITH AP_CHECKS AS (
  SELECT
    app.invoice_id,
    app.creation_date AS payment_date,
    app.payment_num,
    app.amount AS payment_amount,
    app.posted_flag,
    apc.check_number,
    apc.check_date
  FROM silver_bec_ods.ap_invoice_payments_all AS app, silver_bec_ods.ap_checks_all AS apc
  WHERE
    app.check_id = apc.check_id
) /* and app.org_id = 85 */
INSERT INTO gold_bec_dwh_rpt.FACT_AP_INVOICE_DETAILS_RT /* 3806330 */
(
  SELECT DISTINCT
    api.vendor_id,
    api.vendor_site_id,
    api.invoice_id AS ap_invoice_id,
    gcc.segment2 AS invoice_department,
    gcc.segment3 AS invoice_account,
    api.invoice_num,
    api.invoice_date,
    ail.line_number,
    ail.amount AS invoice_line_amount,
    aid.amount AS distribution_amount,
    api.invoice_amount AS invoice_total_amount,
    api.invoice_currency_code,
    api.amount_paid,
    ail.project_id,
    ail.task_id,
    api.org_id,
    aca.payment_date,
    aca.payment_num,
    aca.payment_amount,
    aca.check_number,
    aca.check_date,
    aca.posted_flag,
    api.DISCOUNT_AMOUNT_TAKEN,
    api.TERMS_DATE,
    api.FREIGHT_AMOUNT,
    api.INVOICE_RECEIVED_DATE,
    api.EXCHANGE_RATE,
    api.EXCHANGE_DATE,
    api.CANCELLED_DATE,
    api.CREATION_DATE,
    api.LAST_UPDATE_DATE,
    api.CANCELLED_AMOUNT,
    api.PAY_CURR_INVOICE_AMOUNT,
    api.GL_DATE,
    api.TOTAL_TAX_AMOUNT,
    api.LEGAL_ENTITY_ID,
    api.PARTY_ID,
    api.PARTY_SITE_ID,
    api.AMOUNT_APPLICABLE_TO_DISCOUNT,
    COALESCE(api.BASE_AMOUNT, api.INVOICE_AMOUNT) AS `INVOICE_BASE_AMOUNT`,
    api.ORIGINAL_PREPAYMENT_AMOUNT,
    CAST(COALESCE(api.invoice_amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_INVOICE_AMOUNT,
    CAST(COALESCE(api.amount_paid, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_AMOUNT_PAID,
    api.INVOICE_TYPE_LOOKUP_CODE,
    aid.POSTED_FLAG AS inv_posted_flag,
    AID.ACCOUNTING_DATE,
    AID.DISTRIBUTION_LINE_NUMBER,
    AID.LINE_TYPE_LOOKUP_CODE,
    AID.DESCRIPTION,
    api.SET_OF_BOOKS_ID AS ledger_id
  FROM silver_bec_ods.ap_invoice_lines_all AS ail, silver_bec_ods.ap_invoices_all AS api, gold_bec_dwh.FACT_AP_INV_DISTRIBUTIONS AS aid, bec_Dwh.dim_gl_accounts AS gcc, silver_bec_ods.GL_DAILY_RATES AS DCR, AP_CHECKS AS ACA
  WHERE
    ail.org_id = api.org_id
    AND ail.invoice_id = api.invoice_id
    AND ail.org_id = aid.org_id
    AND ail.invoice_id = aid.invoice_id
    AND ail.line_number = aid.invoice_line_number
    AND aid.GL_ACCOUNT_ID_KEY = gcc.dw_load_id
    AND ail.invoice_id = aca.INVOICE_ID()
    AND DCR.TO_CURRENCY() = 'USD'
    AND DCR.CONVERSION_TYPE() = 'Corporate'
    AND api.invoice_currency_code = DCR.FROM_CURRENCY()
    AND DCR.CONVERSION_DATE() = api.invoice_date
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ap_invoice_details_rt' AND batch_name = 'ap';