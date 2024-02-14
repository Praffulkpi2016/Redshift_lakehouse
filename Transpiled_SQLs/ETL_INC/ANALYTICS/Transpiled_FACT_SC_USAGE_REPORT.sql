TRUNCATE table gold_bec_dwh.FACT_SC_USAGE_REPORT;
WITH dist AS (
  SELECT
    dist.customer_trx_line_id,
    dist.customer_trx_id,
    MAX(dist.cust_trx_line_gl_dist_id) AS cust_trx_line_gl_dist_id,
    SUM(COALESCE(dist.amount, 0)) AS amount,
    dist.gl_date AS gl_date,
    MAX(dist.event_id) AS event_id,
    MAX(dist.last_update_date) AS last_update_date,
    SUM(COALESCE(dist.acctd_amount, 0)) AS acctd_amount,
    dist.account_class,
    dist.code_combination_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.ra_cust_trx_line_gl_dist_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS dist
  WHERE
    UPPER(dist.account_class) = 'REV' AND NOT AMOUNT IS NULL
  GROUP BY
    dist.gl_date,
    dist.customer_trx_line_id,
    dist.customer_trx_id,
    dist.account_class,
    dist.code_combination_id
), billing_details AS (
  SELECT
    rctl.interface_line_attribute1 AS contract_number,
    bcl.cle_id AS id,
    rctl.CONTRACT_LINE_ID,
    dist.customer_trx_line_id,
    rct.trx_number,
    rct.trx_date,
    rctl.line_number,
    rctl.INTERFACE_LINE_ATTRIBUTE4,
    rctl.INTERFACE_LINE_ATTRIBUTE5,
    rctl.quantity_invoiced AS QUANTITY,
    rctl.INTERFACE_LINE_ATTRIBUTE11,
    rctl.INTERFACE_LINE_ATTRIBUTE12,
    rctl.revenue_amount,
    dist.customer_trx_id,
    dist.cust_trx_line_gl_dist_id,
    dist.amount,
    dist.gl_date,
    dist.event_id,
    dist.last_update_date,
    dist.acctd_amount,
    dist.account_class,
    dist.code_combination_id,
    rct.invoice_currency_code
  FROM (
    SELECT
      *
    FROM silver_bec_ods.oks_bill_cont_lines
    WHERE
      is_deleted_flg <> 'Y'
  ) AS bcl, (
    SELECT
      *
    FROM silver_bec_ods.oks_bill_transactions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS btn, (
    SELECT
      *
    FROM silver_bec_ods.oks_bill_txn_lines
    WHERE
      is_deleted_flg <> 'Y'
  ) AS btl, (
    SELECT
      *
    FROM silver_bec_ods.ra_customer_trx_lines_all
    WHERE
      is_deleted_flg <> 'Y'
  ) AS rctl, (
    SELECT
      *
    FROM silver_bec_ods.ra_customer_trx_all
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
INSERT INTO gold_bec_dwh.FACT_SC_USAGE_REPORT
(
  SELECT
    contract_number,
    hdr_status,
    hdr_start_date,
    hdr_end_date,
    BILLING_TYPE,
    KWH,
    line_number,
    Line_Type,
    line_status,
    ship_to_site_use_id,
    bill_to_site_use_id,
    service_name,
    Currency_Code,
    Contract_Line_Start_Date,
    Contract_Line_End_Date,
    Price_List_Name,
    Start_Date_Active,
    End_Date_Active,
    product_uom_code,
    Unit_Price,
    invoice_number,
    invoice_date,
    invoice_line_number,
    invoice_line_amount,
    bill_from_date,
    bill_to_date,
    net_reading_qty,
    TMO_PERC,
    percentage_factor,
    gl_date,
    cust_trx_line_gl_dist_id,
    id,
    org_id,
    code_combination_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ship_to_site_use_id AS ship_to_site_use_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || bill_to_site_use_id AS bill_to_site_use_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || cust_trx_line_gl_dist_id AS cust_trx_line_gl_dist_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || id AS id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || org_id AS org_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || code_combination_id AS code_combination_id_KEY,
    CAST(COALESCE(invoice_line_amount, 0) * COALESCE(conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_INVOICE_LINE_AMOUNT,
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
    ) || '-' || COALESCE(contract_number, 'NA') || '-' || COALESCE(id, 'NA') || '-' || COALESCE(CUST_TRX_LINE_GL_DIST_ID, 0) || '-' || COALESCE(start_date_active, '1900-01-01 00:00:00') || '-' || COALESCE(end_date_active, '1900-01-01 00:00:00') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT DISTINCT
      OKH.contract_number,
      okh.sts_code AS hdr_status,
      okh.start_date AS hdr_start_date,
      okh.end_date AS hdr_end_date,
      CII.BILLING_TYPE,
      CII.KWH,
      oal.line_number,
      oll.lse_name AS Line_Type,
      oal.sts_code AS line_status,
      oal.ship_to_site_use_id,
      oal.bill_to_site_use_id,
      old1.service_name,
      oal.currency_code AS Currency_Code,
      FLOOR(oal.start_date) AS Contract_Line_Start_Date,
      FLOOR(oal.end_date) AS Contract_Line_End_Date,
      qpl.name AS Price_List_Name,
      FLOOR(qll.start_date_active) AS Start_Date_Active,
      FLOOR(qll.end_date_active) AS End_Date_Active,
      qll.product_uom_code,
      qll.operand AS Unit_Price,
      inv.trx_number AS invoice_number,
      inv.trx_date AS invoice_date,
      inv.line_number AS invoice_line_number,
      inv.revenue_amount AS invoice_line_amount,
      CAST(inv.interface_line_attribute4 AS TIMESTAMP) AS bill_from_date,
      CAST(inv.interface_line_attribute5 AS TIMESTAMP) AS bill_to_date,
      inv.QUANTITY AS net_reading_qty,
      inv.INTERFACE_LINE_ATTRIBUTE11 AS TMO_PERC,
      inv.INTERFACE_LINE_ATTRIBUTE12 AS percentage_factor,
      inv.gl_date,
      inv.cust_trx_line_gl_dist_id,
      oal.id,
      OKH.org_id,
      inv.code_combination_id,
      DCR.conversion_rate
    FROM (
      SELECT
        *
      FROM silver_bec_ods.okc_k_headers_all_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS okh, (
      SELECT
        *
      FROM silver_bec_ods.OKC_K_LINES_B
      WHERE
        is_deleted_flg <> 'Y'
    ) AS oal, (
      SELECT
        CLEB.ID AS ID,
        CLEB.CHR_ID AS CHR_ID,
        CLEB.LINE_NUMBER AS LINE_NUMBER,
        CLEB.STS_CODE AS STS_CODE,
        CLEB.LSE_ID,
        LSET.NAME AS LSE_NAME,
        CLEB.DNZ_CHR_ID AS DNZ_CHR_ID
      FROM (
        SELECT
          *
        FROM silver_bec_ods.OKC_LINE_STYLES_TL
        WHERE
          is_deleted_flg <> 'Y'
      ) AS LSET, (
        SELECT
          *
        FROM silver_bec_ods.OKC_K_LINES_B
        WHERE
          is_deleted_flg <> 'Y'
      ) AS CLEB, (
        SELECT
          *
        FROM silver_bec_ods.OKC_K_LINES_TL
        WHERE
          is_deleted_flg <> 'Y'
      ) AS CLET
      WHERE
        CLEB.ID = CLET.ID AND LSET.ID = CLEB.LSE_ID AND LSET.LANGUAGE = CLET.language
    ) AS oll, (
      SELECT
        *
      FROM silver_bec_ods.qp_pricelists_lov_v
      WHERE
        is_deleted_flg <> 'Y'
    ) AS qpl, (
      SELECT
        *
      FROM silver_bec_ods.oks_line_details_v
      WHERE
        is_deleted_flg <> 'Y'
    ) AS old1, (
      SELECT
        *
      FROM silver_bec_ods.qp_list_lines_v
      WHERE
        is_deleted_flg <> 'Y'
    ) AS qll, (
      SELECT
        *
      FROM silver_bec_ods.fnd_user
      WHERE
        is_deleted_flg <> 'Y'
    ) AS fu, (
      SELECT
        *
      FROM silver_bec_ods.per_all_people_f
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ppf, billing_details AS inv, (
      SELECT
        *
      FROM silver_bec_ods.GL_DAILY_RATES
      WHERE
        is_deleted_flg <> 'Y' AND to_currency = 'USD' AND conversion_type = 'Corporate'
    ) AS DCR, (
      SELECT
        okl1.dnz_chr_id,
        okl1.cle_id,
        CI.ATTRIBUTE4 AS BILLING_TYPE,
        SUM((
          CAST(CI.ATTRIBUTE5 AS DECIMAL(28, 10))
        )) AS KWH
      FROM (
        SELECT
          *
        FROM silver_bec_ods.okc_k_lines_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS okl1, (
        SELECT
          *
        FROM silver_bec_ods.okc_k_items
        WHERE
          is_deleted_flg <> 'Y'
      ) AS oki1, (
        SELECT
          *
        FROM silver_bec_ods.csi_counter_associations
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ca, (
        SELECT
          *
        FROM silver_bec_ods.csi_item_instances
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ci
      WHERE
        NOT okl1.cle_id IS NULL /* AND okl1.dnz_chr_id = oal.chr_id */ /* AND okl1.cle_id = oal.id */
        AND okl1.dnz_chr_id = oki1.dnz_chr_id
        AND okl1.id = oki1.cle_id
        AND oki1.jtot_object1_code = 'OKX_COUNTER'
        AND okl1.STS_CODE <> 'TERMINATED'
        AND ca.counter_id = oki1.object1_id1
        AND ci.instance_id = ca.source_object_id
      GROUP BY
        okl1.dnz_chr_id,
        okl1.cle_id,
        okl1.cle_id,
        CI.ATTRIBUTE4
    ) AS cii
    WHERE
      okh.ID = oal.chr_id
      AND oal.id = oll.id
      AND oal.cle_id IS NULL
      AND qpl.price_list_id = oal.price_list_id
      AND old1.contract_id = oll.chr_id
      AND old1.line_id = oal.id
      AND oal.price_list_id = qll.list_header_id
      AND old1.object1_id1 = qll.PRODUCT_ATTR_VALUE
      AND oll.lse_name = 'Usage'
      AND cii.dnz_chr_id = okh.id
      AND cii.dnz_chr_id = oal.chr_id
      AND cii.cle_id = oal.id
      AND qll.last_updated_by = fu.user_id
      AND fu.employee_id = ppf.person_id
      AND qll.last_update_date BETWEEN ppf.effective_start_date AND ppf.effective_end_date
      AND NOT okh.sts_code IN ('TERMINATED', 'CANCELLED')
      AND oal.sts_code <> 'TERMINATED'
      AND oal.id = inv.ID()
      AND CAST(inv.interface_line_attribute4 AS TIMESTAMP) BETWEEN qll.start_date_active AND qll.end_date_active
      AND inv.invoice_currency_code = DCR.FROM_CURRENCY()
      AND inv.trx_date = DCR.CONVERSION_DATE()
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_sc_usage_report' AND batch_name = 'sc';