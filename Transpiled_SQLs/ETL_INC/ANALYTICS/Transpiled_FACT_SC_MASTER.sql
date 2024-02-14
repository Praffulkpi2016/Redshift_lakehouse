/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_SC_MASTER
WHERE
  (
    (
      COALESCE(contract_id, 0)
    )
  ) IN (
    SELECT
      COALESCE(ODS.contract_id, 0) AS contract_id
    FROM gold_bec_dwh.FACT_SC_MASTER AS dw, (
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
        okh.id AS contract_id
      FROM silver_bec_ods.okc_k_headers_all_b AS okh, silver_bec_ods.okc_k_headers_tl AS okst, silver_bec_ods.oks_k_headers_b AS oks_h, inv_curr AS inv
      WHERE
        1 = 1
        AND okh.id = okst.id
        AND okh.id = oks_h.chr_id
        AND okst.language = 'US'
        AND NOT okh.bill_to_site_use_id IS NULL
        AND okh.contract_number = inv.CONTRACT_NUMBER()
        AND (
          okh.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_sc_master' AND batch_name = 'sc'
          )
          OR okh.is_deleted_flg = 'Y'
        )
      GROUP BY
        okh.id
    ) AS ODS
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.contract_id, 0)
  );
/* Insert records */
WITH inv_curr AS (
  SELECT DISTINCT
    interface_line_attribute1 AS contract_number,
    rct.invoice_currency_code
  FROM silver_bec_ods.ra_customer_trx_lines_all AS rctl, silver_bec_ods.ra_customer_trx_all AS rct
  WHERE
    rctl.interface_line_context = 'OKS CONTRACTS'
    AND rct.customer_trx_id = rctl.customer_trx_id
)
INSERT INTO gold_bec_dwh.FACT_SC_MASTER (
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
  contract_id_key,
  org_id_key,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
    SELECT
      okh.id AS contract_id,
      okh.org_id,
      okh.contract_number,
      okst.description,
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
        FROM (
          SELECT
            *
          FROM silver_bec_ods.ra_terms_tl
          WHERE
            is_deleted_flg <> 'Y'
        )
        WHERE
          term_id = okh.payment_term_id
      ) AS payment_term,
      (
        SELECT
          name
        FROM (
          SELECT
            *
          FROM silver_bec_ods.ra_rules
          WHERE
            is_deleted_flg <> 'Y'
        )
        WHERE
          rule_id = oks_h.acct_rule_id
      ) AS acct_rule_name,
      (
        SELECT
          name
        FROM (
          SELECT
            *
          FROM silver_bec_ods.ra_rules
          WHERE
            is_deleted_flg <> 'Y'
        )
        WHERE
          rule_id = okh.inv_rule_id
      ) AS inv_rule_name,
      okh.creation_date,
      okh.created_by,
      okh.last_update_date,
      okh.last_updated_by,
      okh.bill_to_site_use_id
    FROM (
      SELECT
        *
      FROM silver_bec_ods.okc_k_headers_all_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS okh, (
      SELECT
        *
      FROM silver_bec_ods.okc_k_headers_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS okst, (
      SELECT
        *
      FROM silver_bec_ods.oks_k_headers_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS oks_h, inv_curr AS inv
    WHERE
      1 = 1
      AND okh.id = okst.id
      AND okh.id = oks_h.chr_id
      AND okst.language = 'US'
      AND NOT okh.bill_to_site_use_id IS NULL
      AND okh.contract_number = inv.CONTRACT_NUMBER()
      AND (
        okh.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_sc_master' AND batch_name = 'sc'
        )
      ) /* AND okh.contract_number like 'HOM085%' */
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