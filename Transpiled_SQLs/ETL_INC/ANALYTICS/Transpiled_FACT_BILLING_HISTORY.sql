DELETE FROM gold_bec_dwh.FACT_BILLING_HISTORY
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.hz_cust_accounts AS hca_billto, silver_bec_ods.hz_parties AS hz_billto, silver_bec_ods.hz_cust_accounts AS hca_shipto, silver_bec_ods.hz_cust_acct_sites_all AS hcasa_shipto, silver_bec_ods.hz_cust_site_uses_all AS hcsua_shipto, silver_bec_ods.hz_parties AS hz_shipto, silver_bec_ods.hz_cust_accounts AS lhca_shipto, silver_bec_ods.hz_cust_acct_sites_all AS lhcasa_shipto, silver_bec_ods.hz_cust_site_uses_all AS lhcsua_shipto, silver_bec_ods.hz_parties AS lhz_shipto, silver_bec_ods.hz_locations AS lhl, silver_bec_ods.hz_party_sites AS lhps, silver_bec_ods.ra_customer_trx_all AS ct, silver_bec_ods.hz_party_sites AS hps, silver_bec_ods.hz_locations AS hl, silver_bec_ods.ra_batch_sources_all AS rbsa, silver_bec_ods.ra_cust_trx_types_all AS rctta, silver_bec_ods.ra_cust_trx_line_gl_dist_all AS rgd, silver_bec_ods.ra_terms_tl AS rt, silver_bec_ods.ra_customer_trx_lines_all AS rctla, (
      SELECT
        a.cognomen AS cognomen,
        b.contract_number
      FROM silver_bec_ods.okc_k_headers_tl AS a, silver_bec_ods.okc_k_headers_all_b AS b
      WHERE
        a.id = b.id AND b.sts_code = 'ACTIVE'
    ) AS pnso, (
      SELECT
        MAX(COALESCE(t.quantity_invoiced, t.quantity_credited) * t.unit_selling_price) AS tax_amount,
        t.customer_trx_id,
        rgd.customer_trx_line_id
      FROM (
        SELECT
          *
        FROM silver_bec_ods.ra_customer_trx_lines_all
        WHERE
          1 = 1
      ) AS t, (
        SELECT
          *
        FROM silver_bec_ods.ra_cust_trx_line_gl_dist_all
        WHERE
          1 = 1
      ) AS rgd
      WHERE
        1 = 1
        AND rgd.code_combination_id = '1025'
        AND t.customer_trx_line_id = rgd.customer_trx_line_id
      GROUP BY
        t.customer_trx_id,
        rgd.customer_trx_line_id
    ) AS tax_amount_db1, (
      SELECT
        SUM(COALESCE(tl.extended_amount, 0)) AS tax_amount,
        tl.link_to_cust_trx_line_id
      FROM (
        SELECT
          *
        FROM silver_bec_ods.ra_customer_trx_lines_all
        WHERE
          1 = 1
      ) AS tl
      WHERE
        1 = 1 AND tl.line_type = 'TAX'
      GROUP BY
        tl.link_to_cust_trx_line_id
    ) AS tax_amount_db2, (
      SELECT
        MAX(COALESCE(gl_date, '1900-01-01 12:00:00')) AS gl_date,
        rgds.customer_trx_line_id
      FROM (
        SELECT
          *
        FROM silver_bec_ods.ra_cust_trx_line_gl_dist_all
        WHERE
          1 = 1
      ) AS rgds
      GROUP BY
        rgds.customer_trx_line_id
    ) AS rgds_gl
    WHERE
      1 = 1
      AND (
        ct.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_billing_history' AND batch_name = 'ar'
        )
        OR rctla.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_billing_history' AND batch_name = 'ar'
        )
        OR rgd.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_billing_history' AND batch_name = 'ar'
        )
      )
      AND ct.bill_to_customer_id = hca_billto.cust_account_id
      AND hca_billto.party_id = hz_billto.party_id
      AND ct.ship_to_customer_id = hca_shipto.CUST_ACCOUNT_ID()
      AND hca_shipto.party_id = hz_shipto.PARTY_ID()
      AND ct.ship_to_site_use_id = hcsua_shipto.SITE_USE_ID()
      AND hcsua_shipto.cust_acct_site_id = hcasa_shipto.CUST_ACCT_SITE_ID()
      AND hcasa_shipto.party_site_id = hps.PARTY_SITE_ID()
      AND hps.location_id = hl.LOCATION_ID()
      AND rctla.ship_to_customer_id = lhca_shipto.CUST_ACCOUNT_ID()
      AND lhca_shipto.party_id = lhz_shipto.PARTY_ID()
      AND rctla.ship_to_site_use_id = lhcsua_shipto.SITE_USE_ID()
      AND lhcsua_shipto.cust_acct_site_id = lhcasa_shipto.CUST_ACCT_SITE_ID()
      AND lhcasa_shipto.party_site_id = lhps.PARTY_SITE_ID()
      AND lhps.location_id = lhl.LOCATION_ID()
      AND hcasa_shipto.party_site_id = hps.PARTY_SITE_ID()
      AND ct.batch_source_id = rbsa.batch_source_id
      AND ct.cust_trx_type_id = rctta.cust_trx_type_id
      AND ct.term_id = rt.TERM_ID()
      AND rbsa.org_id = ct.org_id
      AND ct.org_id = rctta.org_id
      AND rctla.customer_trx_line_id = rgd.customer_trx_line_id
      AND ct.customer_trx_id = rctla.customer_trx_id
      AND rctla.line_type = 'LINE'
      AND ct.interface_header_attribute1 = pnso.CONTRACT_NUMBER()
      AND rctla.customer_trx_line_id = tax_amount_db1.CUSTOMER_TRX_LINE_ID()
      AND rctla.customer_trx_line_id = tax_amount_db2.LINK_TO_CUST_TRX_LINE_ID()
      AND rctla.customer_trx_line_id = rgds_gl.CUSTOMER_TRX_LINE_ID()
      AND FACT_BILLING_HISTORY.DW_LOAD_ID = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ct.customer_trx_id, 0) || '-' || COALESCE(rctla.customer_trx_line_id, 0) || '-' || COALESCE(rgd.CUST_TRX_LINE_GL_DIST_ID, 0)
  );
INSERT INTO gold_bec_dwh.FACT_BILLING_HISTORY
(
  SELECT
    bill_to_account,
    bill_to_customer_name,
    ship_to_account,
    ship_to_customer_name,
    ship_to_address1,
    ship_to_country,
    state,
    city,
    zip,
    source,
    transaction_type,
    invoice_number,
    line_number,
    line_type,
    gl_date,
    due_date,
    invoice_date,
    payment_terms,
    purchase_order,
    project_num_or_sales_order,
    sales_order_date,
    reference,
    item,
    inventory_item_id,
    item_id_key,
    item_desc,
    uom,
    billed_quantity,
    unit_price,
    extended_amount,
    tax_amount,
    unearned_account,
    tax_account,
    receivables_account,
    inv_currency_code,
    org_id,
    org_id_key,
    customer_trx_id,
    customer_trx_line_id,
    CAST(COALESCE(a.extended_amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_EXTENDED_AMOUNT,
    CAST(COALESCE(a.tax_amount, 0) * COALESCE(DCR.conversion_rate, 1) AS DECIMAL(18, 2)) AS GBL_TAX_AMOUNT,
    'N' AS IS_DELETED_FLG,
    source_app_id,
    dw_load_id,
    dw_insert_date,
    dw_update_date
  FROM (
    SELECT DISTINCT
      hca_billto.account_number AS bill_to_account,
      hz_billto.party_name AS bill_to_customer_name,
      COALESCE(hca_shipto.account_number, lhca_shipto.account_number) AS ship_to_account,
      COALESCE(hz_shipto.party_name, lhz_shipto.party_name) AS ship_to_customer_name,
      COALESCE(hl.address1, lhl.address1) AS ship_to_address1,
      COALESCE(hl.country, lhl.country) AS ship_to_country,
      COALESCE(hl.state, lhl.state) AS state,
      COALESCE(hl.city, lhl.city) AS city,
      COALESCE(hl.postal_code, lhl.postal_code) AS zip,
      rbsa.name AS source,
      rctta.name AS transaction_type,
      ct.trx_number AS invoice_number,
      rctla.line_number,
      rctla.line_type,
      rgds_gl.gl_date AS gl_date,
      ct.term_due_date AS due_date,
      FLOOR(ct.trx_date) AS invoice_date,
      rt.name AS payment_terms,
      ct.purchase_order,
      CASE
        WHEN rbsa.name = 'PROJECTS INVOICES'
        THEN ct.interface_header_attribute1
        WHEN rbsa.name = 'OKS_CONTRACTS'
        THEN pnso.cognomen
      END AS project_num_or_sales_order,
      rctla.sales_order_date,
      CASE
        WHEN rbsa.name = 'OKS_CONTRACTS' AND rctta.name = 'Invoice-OKS'
        THEN rctla.sales_order
        WHEN rbsa.name = 'PROJECTS INVOICES'
        AND rctta.name = 'Projects Invoices'
        AND ct.interface_header_context = 'PROJECTS INVOICES'
        THEN ct.interface_header_attribute1
        ELSE ct.ct_reference
      END AS reference,
      (
        SELECT
          segment1
        FROM silver_bec_ods.mtl_system_items_b
        WHERE
          inventory_item_id = rctla.inventory_item_id
        LIMIT 1
      ) AS item,
      rctla.inventory_item_id,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) || '-' || rctla.inventory_item_id AS ITEM_ID_KEY,
      rctla.description AS item_desc,
      rctla.uom_code AS uom,
      CASE
        WHEN rctta.name = 'Manual Credit Memo'
        THEN rctla.quantity_credited
        ELSE rctla.quantity_invoiced
      END AS billed_quantity,
      rctla.unit_selling_price AS unit_price,
      (
        SELECT
          MAX(rctla.extended_amount) /* ,rctla.customer_trx_line_id,rgd1.cust_trx_line_gl_dist_id */
        FROM silver_bec_ods.ra_customer_trx_lines_all AS t, silver_bec_ods.ra_cust_trx_line_gl_dist_all AS rgd1, silver_bec_ods.ra_customer_trx_lines_all AS rctla
        WHERE
          t.customer_trx_id = ct.customer_trx_id
          AND t.customer_trx_line_id = rgd1.customer_trx_line_id
          AND rgd1.customer_trx_line_id = rctla.customer_trx_line_id
          AND rgd.cust_trx_line_gl_dist_id = rgd1.cust_trx_line_gl_dist_id
          AND rgd1.code_combination_id <> '1025'
        GROUP BY
          rctla.customer_trx_line_id,
          rgd1.cust_trx_line_gl_dist_id
      ) AS extended_amount,
      CASE
        WHEN rgd.code_combination_id = '1025'
        THEN tax_amount_db1.tax_amount
        ELSE tax_amount_db2.tax_amount
      END AS tax_amount,
      (
        SELECT
          gcck.concatenated_segments
        FROM silver_bec_ods.gl_code_combinations_kfv AS gcck, silver_bec_ods.ra_cust_trx_line_gl_dist_all AS rgd
        WHERE
          gcck.code_combination_id = rgd.code_combination_id
          AND rgd.account_class = 'UNEARN'
          AND rgd.customer_trx_line_id = rctla.customer_trx_line_id
        GROUP BY
          gcck.concatenated_segments
        HAVING
          SUM(rgd.`percent`) = 100
      ) AS unearned_account,
      (
        SELECT
          gcck.concatenated_segments
        FROM silver_bec_ods.ra_customer_trx_lines_all AS tl, silver_bec_ods.ra_cust_trx_line_gl_dist_all AS b, silver_bec_ods.gl_code_combinations_kfv AS gcck
        WHERE
          tl.line_type = 'TAX'
          AND tl.customer_trx_id = rctla.customer_trx_id
          AND tl.link_to_cust_trx_line_id = rctla.customer_trx_line_id
          AND tl.customer_trx_id = b.customer_trx_id
          AND tl.customer_trx_line_id = b.customer_trx_line_id
          AND b.code_combination_id = gcck.code_combination_id
        LIMIT 1 /*            AND ROWNUM = 1 */
      ) AS tax_account,
      (
        SELECT DISTINCT
          gcck.concatenated_segments
        FROM silver_bec_ods.gl_code_combinations_kfv AS gcck, silver_bec_ods.ra_cust_trx_line_gl_dist_all AS rgd
        WHERE
          gcck.code_combination_id = rgd.code_combination_id
          AND rgd.account_class = 'REC'
          AND rgd.customer_trx_id = ct.customer_trx_id
        LIMIT 1 /*            AND ROWNUM = 1 */
      ) AS receivables_account,
      ct.INVOICE_CURRENCY_CODE AS INV_CURRENCY_CODE,
      ct.org_id,
      (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || ct.org_id AS ORG_ID_KEY,
      ct.customer_trx_id,
      rctla.customer_trx_line_id,
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
      ) || '-' || COALESCE(ct.customer_trx_id, 0) || '-' || COALESCE(rctla.customer_trx_line_id, 0) || '-' || COALESCE(rgd.CUST_TRX_LINE_GL_DIST_ID, 0) AS dw_load_id,
      CURRENT_TIMESTAMP() AS dw_insert_date,
      CURRENT_TIMESTAMP() AS dw_update_date
    FROM (
      SELECT
        account_number,
        cust_account_id,
        party_id
      FROM silver_bec_ods.hz_cust_accounts
      WHERE
        is_deleted_flg <> 'Y'
    ) AS hca_billto, (
      SELECT
        party_id,
        party_name
      FROM silver_bec_ods.hz_parties
      WHERE
        is_deleted_flg <> 'Y'
    ) AS hz_billto, (
      SELECT
        cust_account_id,
        party_id,
        account_number
      FROM silver_bec_ods.hz_cust_accounts
      WHERE
        is_deleted_flg <> 'Y'
    ) AS hca_shipto, (
      SELECT
        cust_acct_site_id,
        party_site_id
      FROM silver_bec_ods.hz_cust_acct_sites_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS hcasa_shipto, (
      SELECT
        cust_acct_site_id,
        site_use_id
      FROM silver_bec_ods.hz_cust_site_uses_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS hcsua_shipto, (
      SELECT
        party_id,
        party_name
      FROM silver_bec_ods.hz_parties
      WHERE
        is_deleted_flg <> 'Y'
    ) AS hz_shipto, (
      SELECT
        cust_account_id,
        party_id,
        account_number
      FROM silver_bec_ods.hz_cust_accounts
      WHERE
        is_deleted_flg <> 'Y'
    ) AS lhca_shipto, (
      SELECT
        cust_acct_site_id,
        party_site_id
      FROM silver_bec_ods.hz_cust_acct_sites_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS lhcasa_shipto, (
      SELECT
        cust_acct_site_id,
        site_use_id
      FROM silver_bec_ods.hz_cust_site_uses_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS lhcsua_shipto, (
      SELECT
        party_id,
        party_name
      FROM silver_bec_ods.hz_parties
      WHERE
        is_deleted_flg <> 'Y'
    ) AS lhz_shipto, (
      SELECT
        *
      FROM silver_bec_ods.hz_locations
      WHERE
        is_deleted_flg <> 'Y'
    ) AS lhl, (
      SELECT
        location_id,
        party_site_id
      FROM silver_bec_ods.hz_party_sites
      WHERE
        is_deleted_flg <> 'Y'
    ) AS lhps, (
      SELECT
        *
      FROM silver_bec_ods.ra_customer_trx_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ct, (
      SELECT
        location_id,
        party_site_id
      FROM silver_bec_ods.hz_party_sites
      WHERE
        is_deleted_flg <> 'Y'
    ) AS hps, (
      SELECT
        *
      FROM silver_bec_ods.hz_locations
      WHERE
        is_deleted_flg <> 'Y'
    ) AS hl, (
      SELECT
        batch_source_id,
        org_id,
        name
      FROM silver_bec_ods.ra_batch_sources_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS rbsa, (
      SELECT
        cust_trx_type_id,
        org_id,
        name
      FROM silver_bec_ods.ra_cust_trx_types_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS rctta, (
      SELECT
        customer_trx_line_id,
        CUST_TRX_LINE_GL_DIST_ID,
        code_combination_id,
        kca_seq_date
      FROM silver_bec_ods.ra_cust_trx_line_gl_dist_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS rgd, (
      SELECT
        *
      FROM silver_bec_ods.ra_terms_tl
      WHERE
        language = 'US' AND is_deleted_flg <> 'Y'
    ) AS rt, (
      SELECT
        *
      FROM silver_bec_ods.ra_customer_trx_lines_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS rctla, (
      SELECT
        a.cognomen AS cognomen,
        b.contract_number
      FROM (
        SELECT
          *
        FROM silver_bec_ods.okc_k_headers_tl
        WHERE
          is_deleted_flg <> 'Y'
      ) AS a, (
        SELECT
          *
        FROM silver_bec_ods.okc_k_headers_all_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS b
      WHERE
        a.id = b.id AND b.sts_code = 'ACTIVE'
    ) AS pnso, (
      SELECT
        MAX(COALESCE(t.quantity_invoiced, t.quantity_credited) * t.unit_selling_price) AS tax_amount,
        t.customer_trx_id,
        rgd.customer_trx_line_id
      FROM (
        SELECT
          *
        FROM silver_bec_ods.ra_customer_trx_lines_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS t, (
        SELECT
          *
        FROM silver_bec_ods.ra_cust_trx_line_gl_dist_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS rgd
      WHERE
        1 = 1
        AND rgd.code_combination_id = '1025'
        AND t.customer_trx_line_id = rgd.customer_trx_line_id
      GROUP BY
        t.customer_trx_id,
        rgd.customer_trx_line_id
    ) AS tax_amount_db1, (
      SELECT
        SUM(COALESCE(tl.extended_amount, 0)) AS tax_amount,
        tl.link_to_cust_trx_line_id
      FROM (
        SELECT
          *
        FROM silver_bec_ods.ra_customer_trx_lines_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS tl
      WHERE
        1 = 1 AND tl.line_type = 'TAX'
      GROUP BY
        tl.link_to_cust_trx_line_id
    ) AS tax_amount_db2, (
      SELECT
        MAX(COALESCE(gl_date, '1900-01-01 12:00:00')) AS gl_date,
        rgds.customer_trx_line_id
      FROM (
        SELECT
          *
        FROM silver_bec_ods.ra_cust_trx_line_gl_dist_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS rgds
      GROUP BY
        rgds.customer_trx_line_id
    ) AS rgds_gl
    WHERE
      ct.bill_to_customer_id = hca_billto.cust_account_id
      AND hca_billto.party_id = hz_billto.party_id
      AND ct.ship_to_customer_id = hca_shipto.CUST_ACCOUNT_ID()
      AND hca_shipto.party_id = hz_shipto.PARTY_ID()
      AND ct.ship_to_site_use_id = hcsua_shipto.SITE_USE_ID()
      AND hcsua_shipto.cust_acct_site_id = hcasa_shipto.CUST_ACCT_SITE_ID()
      AND hcasa_shipto.party_site_id = hps.PARTY_SITE_ID()
      AND hps.location_id = hl.LOCATION_ID()
      AND rctla.ship_to_customer_id = lhca_shipto.CUST_ACCOUNT_ID()
      AND lhca_shipto.party_id = lhz_shipto.PARTY_ID()
      AND rctla.ship_to_site_use_id = lhcsua_shipto.SITE_USE_ID()
      AND lhcsua_shipto.cust_acct_site_id = lhcasa_shipto.CUST_ACCT_SITE_ID()
      AND lhcasa_shipto.party_site_id = lhps.PARTY_SITE_ID()
      AND lhps.location_id = lhl.LOCATION_ID()
      AND hcasa_shipto.party_site_id = hps.PARTY_SITE_ID()
      AND ct.batch_source_id = rbsa.batch_source_id
      AND ct.cust_trx_type_id = rctta.cust_trx_type_id
      AND ct.term_id = rt.TERM_ID()
      AND rbsa.org_id = ct.org_id
      AND ct.org_id = rctta.org_id
      AND rctla.customer_trx_line_id = rgd.customer_trx_line_id
      AND ct.customer_trx_id = rctla.customer_trx_id
      AND rctla.line_type = 'LINE'
      AND ct.interface_header_attribute1 = pnso.CONTRACT_NUMBER()
      AND rctla.customer_trx_line_id = tax_amount_db1.CUSTOMER_TRX_LINE_ID()
      AND rctla.customer_trx_line_id = tax_amount_db2.LINK_TO_CUST_TRX_LINE_ID()
      AND rctla.customer_trx_line_id = rgds_gl.CUSTOMER_TRX_LINE_ID()
      AND (
        ct.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_billing_history' AND batch_name = 'ar'
        )
        OR rctla.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_billing_history' AND batch_name = 'ar'
        )
        OR rgd.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_billing_history' AND batch_name = 'ar'
        )
      )
  ) AS a
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.GL_DAILY_RATES
    WHERE
      to_currency = 'USD' AND conversion_type = 'Corporate' AND is_deleted_flg <> 'Y'
  ) AS DCR
    ON a.INV_CURRENCY_CODE = DCR.from_currency AND a.gl_date = DCR.conversion_date
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_billing_history' AND batch_name = 'ar';