DROP table IF EXISTS gold_bec_dwh.FACT_SC_MASTER;
CREATE TABLE gold_bec_dwh.FACT_SC_MASTER AS
(
  SELECT
    contract_id,
    org_id,
    contract_number,
    description,
    header_status,
    cust_po_number,
    start_date,
    end_date,
    service_contract_currency,
    estimated_amount,
    invoice_currency_code,
    payment_term,
    acct_rule_name,
    inv_rule_name,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    bill_to_site_use_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || contract_id AS contract_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || org_id AS org_id_key,
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
    ) || '-' || COALESCE(contract_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    WITH inv_curr AS (
      SELECT DISTINCT
        interface_line_attribute1 AS contract_number,
        rct.invoice_currency_code
      FROM silver_bec_ods.ra_customer_trx_lines_all AS rctl, silver_bec_ods.ra_customer_trx_all AS rct
      WHERE
        rctl.interface_line_context = 'OKS CONTRACTS'
        AND rct.customer_trx_id = rctl.customer_trx_id
    )
    SELECT
      okh.id AS contract_id,
      okh.org_id,
      okh.contract_number,
      okst.short_description AS description,
      okh.sts_code AS header_status,
      okh.cust_po_number,
      okh.start_date,
      okh.end_date,
      okh.currency_code AS service_contract_currency,
      okh.estimated_amount,
      inv.invoice_currency_code,
      (
        SELECT
          name
        FROM silver_bec_ods.ra_terms_tl
        WHERE
          term_id = okh.payment_term_id
      ) AS payment_term,
      (
        SELECT
          name
        FROM silver_bec_ods.ra_rules
        WHERE
          rule_id = oks_h.acct_rule_id
      ) AS acct_rule_name,
      (
        SELECT
          name
        FROM silver_bec_ods.ra_rules
        WHERE
          rule_id = okh.inv_rule_id
      ) AS inv_rule_name,
      okh.creation_date,
      okh.created_by,
      okh.last_update_date,
      okh.last_updated_by,
      okh.bill_to_site_use_id
    FROM silver_bec_ods.okc_k_headers_all_b AS okh, silver_bec_ods.okc_k_headers_tl AS okst, silver_bec_ods.oks_k_headers_b AS oks_h, inv_curr AS inv
    WHERE
      1 = 1
      AND okh.id = okst.id
      AND okh.id = oks_h.chr_id
      AND okst.language = 'US'
      AND NOT okh.bill_to_site_use_id IS NULL
      AND okh.contract_number = inv.CONTRACT_NUMBER()
    GROUP BY
      okh.id,
      okh.org_id,
      okh.contract_number,
      okst.description,
      okh.sts_code,
      okh.cust_po_number,
      okh.start_date,
      okh.end_date,
      okh.currency_code,
      okh.estimated_amount,
      inv.invoice_currency_code,
      okh.payment_term_id,
      oks_h.acct_rule_id,
      okh.inv_rule_id,
      okh.creation_date,
      okh.created_by,
      okh.last_update_date,
      okh.last_updated_by,
      okh.bill_to_site_use_id,
      okst.short_description
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_sc_master' AND batch_name = 'sc';