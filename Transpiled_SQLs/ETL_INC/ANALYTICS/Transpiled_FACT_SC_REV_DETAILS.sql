TRUNCATE table gold_bec_dwh.FACT_SC_REV_DETAILS;
WITH dist AS (
  SELECT
    dist.customer_trx_line_id,
    dist.customer_trx_id,
    MAX(dist.cust_trx_line_gl_dist_id) AS cust_trx_line_gl_dist_id,
    SUM(dist.amount) AS amount,
    dist.gl_date AS gl_date,
    MAX(dist.event_id) AS event_id,
    MAX(dist.last_update_date) AS last_update_date,
    SUM(dist.acctd_amount) AS acctd_amount,
    dist.code_combination_id,
    dist.account_class
  FROM (
    SELECT
      *
    FROM BEC_ODS.ra_cust_trx_line_gl_dist_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS dist
  WHERE
    UPPER(dist.account_class) = 'REV' AND NOT amount IS NULL
  GROUP BY
    dist.gl_date,
    dist.customer_trx_line_id,
    dist.customer_trx_id,
    dist.code_combination_id,
    dist.account_class
), billing_details AS (
  SELECT
    rctl.interface_line_attribute1 AS contract_number,
    bcl.cle_id AS id,
    rctl.contract_line_id,
    dist.customer_trx_line_id,
    rct.trx_number,
    rct.trx_date,
    rct.invoice_currency_code,
    rct.exchange_rate,
    rctl.line_number,
    rctl.interface_line_attribute4,
    rctl.interface_line_attribute5,
    rctl.quantity_invoiced,
    rctl.unit_selling_price,
    rctl.interface_line_attribute11,
    rctl.interface_line_attribute12,
    dist.customer_trx_id,
    dist.cust_trx_line_gl_dist_id,
    dist.amount,
    dist.gl_date,
    dist.event_id,
    dist.last_update_date,
    dist.acctd_amount,
    dist.account_class,
    dist.code_combination_id
  FROM (
    SELECT
      *
    FROM BEC_ODS.oks_bill_cont_lines
    WHERE
      is_deleted_flg <> 'Y'
  ) AS bcl, (
    SELECT
      *
    FROM BEC_ODS.oks_bill_transactions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS btn, (
    SELECT
      *
    FROM BEC_ODS.oks_bill_txn_lines
    WHERE
      is_deleted_flg <> 'Y'
  ) AS btl, (
    SELECT
      *
    FROM BEC_ODS.ra_customer_trx_lines_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rctl, (
    SELECT
      *
    FROM BEC_ODS.ra_customer_trx_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rct, dist
  WHERE
    btn.id = bcl.btn_id
    AND btl.btn_id = btn.id
    AND NOT btl.bill_instance_number IS NULL
    AND btl.bill_instance_number = rctl.interface_line_attribute3
    AND rctl.interface_line_context = 'OKS CONTRACTS'
    AND rct.customer_trx_id = rctl.customer_trx_id
    AND dist.customer_trx_line_id = rctl.customer_trx_line_id
    AND dist.account_class = 'REV'
)
INSERT INTO gold_bec_dwh.FACT_SC_REV_DETAILS
(
  SELECT
    contract_number,
    sts_code,
    hdr_start_date,
    hdr_end_date,
    contract_id,
    contract_line_id,
    contract_line_number,
    contract_sub_line_id,
    line_sts_code,
    currency_code,
    line_start_date,
    line_end_date,
    ship_to_site_use_id,
    bill_to_site_use_id,
    price_negotiated,
    customer_trx_line_id,
    gl_dist_id,
    event_id,
    transaction_currency,
    invoice_number,
    invoice_date,
    line_number,
    gl_date,
    code_combination_id,
    period_name,
    invoice_amount,
    revenue_amount_trans_curr,
    acctd_amount,
    revenue_amount_acctd_curr,
    conversion_rate,
    tmo_prcntge,
    bill_from_date,
    bill_to_date,
    date_transaction,
    date_to_interface,
    level_billing_amount,
    line_type,
    org_id,
    service_name,
    gbl_invoice_amount,
    gbl_acctd_amount,
    gbl_level_billing_amount,
    'N' AS is_deleted_flg, /* audit columns */
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
    ) || '-' || COALESCE(contract_id, 0) || '-' || COALESCE(contract_line_id, 'NA') || '-' || COALESCE(customer_trx_line_id, 0) || '-' || COALESCE(date_transaction, CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)) || '-' || COALESCE(date_to_interface, CAST('1900-01-01 00:00:00.000' AS TIMESTAMP)) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      hdr.contract_number,
      hdr.sts_code,
      hdr.start_date AS hdr_start_date,
      hdr.end_date AS hdr_end_date,
      hdr.id AS contract_id,
      okl.id AS contract_line_id,
      okl.line_number AS contract_line_number,
      okl1.id AS contract_sub_line_id,
      okl.sts_code AS line_sts_code,
      okl.currency_code,
      okl.start_date AS line_start_date,
      okl.end_date AS line_end_date,
      okl.ship_to_site_use_id,
      okl.bill_to_site_use_id,
      okl.price_negotiated,
      inv.customer_trx_line_id,
      inv.cust_trx_line_gl_dist_id AS gl_dist_id,
      inv.event_id,
      inv.invoice_currency_code AS transaction_currency,
      inv.trx_number AS invoice_number,
      inv.trx_date AS invoice_date,
      inv.line_number,
      inv.gl_date AS gl_date,
      inv.code_combination_id,
      DATE_FORMAT(inv.gl_date, 'MON-yy') AS period_name,
      SUM(COALESCE(inv.amount, 0)) AS invoice_amount,
      SUM((
        COALESCE(inv.quantity_invoiced, 0) * inv.unit_selling_price
      )) AS revenue_amount_trans_curr,
      SUM(COALESCE(inv.acctd_amount, 0)) AS acctd_amount,
      (
        CASE WHEN inv.exchange_rate IS NULL THEN 1 ELSE inv.exchange_rate END * SUM((
          COALESCE(inv.quantity_invoiced, 0) * inv.unit_selling_price
        ))
      ) AS revenue_amount_acctd_curr,
      CASE WHEN inv.exchange_rate IS NULL THEN 1 ELSE inv.exchange_rate END AS conversion_rate,
      inv.interface_line_attribute11 AS tmo_prcntge,
      CAST(inv.interface_line_attribute4 AS TIMESTAMP) AS bill_from_date,
      CAST(inv.interface_line_attribute5 AS TIMESTAMP) AS bill_to_date,
      lvl.date_transaction,
      lvl.date_to_interface,
      SUM(COALESCE(lvl.amount, 0)) AS level_billing_amount,
      lset.name AS line_type,
      hdr.org_id,
      oldv.service_name,
      SUM(
        CAST(COALESCE(inv.amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2))
      ) AS GBL_INVOICE_AMOUNT,
      SUM(
        CAST(COALESCE(inv.acctd_amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2))
      ) AS GBL_ACCTD_AMOUNT,
      SUM(
        CAST(COALESCE(lvl.amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2))
      ) AS GBL_LEVEL_BILLING_AMOUNT
    FROM (
      SELECT
        *
      FROM BEC_ODS.okc_k_headers_all_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS hdr, (
      SELECT
        *
      FROM BEC_ODS.okc_k_lines_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS okl, (
      SELECT
        *
      FROM BEC_ODS.okc_k_lines_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS okl1, (
      SELECT
        *
      FROM BEC_ODS.okc_line_styles_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS lset, (
      SELECT
        *
      FROM BEC_ODS.OKS_LINE_DETAILS_V
      WHERE
        is_deleted_flg <> 'Y'
    ) AS oldv, (
      SELECT
        cle_id,
        MAX(lvl.date_transaction) AS date_transaction,
        MAX(lvl.date_to_interface) AS date_to_interface,
        SUM(amount) AS amount
      FROM silver_bec_ods.oks_level_elements AS lvl
      GROUP BY
        cle_id
    ) AS lvl, billing_details AS inv, (
      SELECT
        *
      FROM silver_bec_ods.GL_DAILY_RATES
      WHERE
        is_deleted_flg <> 'Y' AND to_currency = 'USD' AND conversion_type = 'Corporate'
    ) AS DCR
    WHERE
      1 = 1
      AND hdr.id = okl.chr_id
      AND okl.cle_id IS NULL
      AND okl.id = inv.id
      AND inv.contract_line_id = okl1.id
      AND okl1.dnz_chr_id = hdr.id
      AND NOT okl1.cle_id IS NULL
      AND okl1.dnz_chr_id = okl.chr_id
      AND okl1.cle_id = okl.id
      AND lvl.cle_id = okl.id
      AND lset.id = okl.lse_id
      AND lset.id = 1 /* added for service contracts */
      AND oldv.contract_id = hdr.id
      AND oldv.line_id = okl.id
      AND inv.invoice_currency_code = DCR.FROM_CURRENCY()
      AND DCR.CONVERSION_DATE() = inv.trx_date
    GROUP BY
      hdr.contract_number,
      hdr.sts_code,
      hdr.start_date,
      hdr.end_date,
      hdr.id,
      okl.id,
      okl.line_number,
      okl1.id,
      okl.sts_code,
      okl.currency_code,
      okl.start_date,
      okl.end_date,
      okl.ship_to_site_use_id,
      okl.bill_to_site_use_id,
      okl.price_negotiated,
      inv.customer_trx_line_id,
      inv.cust_trx_line_gl_dist_id,
      inv.event_id,
      inv.invoice_currency_code,
      inv.trx_number,
      inv.trx_date,
      inv.line_number,
      inv.gl_date,
      inv.code_combination_id,
      DATE_FORMAT(inv.gl_date, 'MON-yy'),
      inv.exchange_rate,
      inv.interface_line_attribute11,
      inv.interface_line_attribute4,
      inv.interface_line_attribute5,
      lvl.date_transaction,
      lvl.date_to_interface,
      lset.name,
      hdr.org_id,
      oldv.service_name
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_sc_rev_details' AND batch_name = 'sc';