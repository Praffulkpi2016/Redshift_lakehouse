DROP table IF EXISTS gold_bec_dwh.FACT_AP_CASH_BURN;
CREATE TABLE gold_bec_dwh.FACT_AP_CASH_BURN AS
(
  SELECT
    org_id,
    check_number,
    vendor_name,
    check_date,
    Amount,
    check_id,
    invoice_id,
    INVOICE_PAYMENT_ID,
    status_lookup_code,
    distribution_amount, /*  gcc.segment1 company_code,
               gcc.segment2 deptartment,
               gcc.segment3 core_account,
               gcc.segment4 intercompany,
               gcc.segment5 budget_id,
               gcc.segment6 LOCATION, */
    PO_DISTRIBUTION_ID,
    dist_code_combination_id,
    project_id,
    task_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || check_id AS check_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || invoice_id AS invoice_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || INVOICE_PAYMENT_ID AS INVOICE_PAYMENT_ID_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || PO_DISTRIBUTION_ID AS PO_DISTRIBUTION_ID_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || dist_code_combination_id AS dist_code_combination_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || project_id AS project_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || task_id AS task_id_key,
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
    ) || '-' || COALESCE(INVOICE_ID, 0) || '-' || COALESCE(dist_code_combination_id, 0) || '-' || COALESCE(PO_DISTRIBUTION_ID, 0) || '-' || COALESCE(INVOICE_PAYMENT_ID, 0) || '-' || COALESCE(project_id, 0) || '-' || COALESCE(task_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      aip.org_id,
      ac.check_number,
      ac.vendor_name,
      ac.check_date,
      aip.amount AS Amount,
      ac.check_id,
      aip.invoice_id,
      aip.INVOICE_PAYMENT_ID,
      ac.status_lookup_code,
      inv.distribution_amount,
      inv.PO_DISTRIBUTION_ID,
      inv.dist_code_combination_id,
      inv.project_id,
      inv.task_id
    FROM (
      SELECT
        *
      FROM silver_bec_ods.AP_CHECKS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ac, (
      SELECT
        *
      FROM silver_bec_ods.AP_INVOICE_PAYMENTS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS AIP, (
      SELECT
        AIA.INVOICE_ID,
        AID.dist_code_combination_id,
        SUM(AID.AMOUNT) AS distribution_amount,
        AID.project_id,
        AID.TASK_ID,
        AID.PO_DISTRIBUTION_ID
      FROM (
        SELECT
          *
        FROM silver_bec_ods.AP_INVOICES_ALL
        WHERE
          is_deleted_flg <> 'Y'
      ) AS AIA, (
        SELECT
          *
        FROM silver_bec_ods.AP_INVOICE_LINES_ALL
        WHERE
          is_deleted_flg <> 'Y'
      ) AS AILA, (
        SELECT
          *
        FROM silver_bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL
        WHERE
          is_deleted_flg <> 'Y'
      ) AS AID
      WHERE
        AIA.INVOICE_ID = AID.INVOICE_ID
        AND AIA.INVOICE_ID = AILA.INVOICE_ID
        AND AILA.LINE_NUMBER = AID.INVOICE_LINE_NUMBER
      GROUP BY
        AIA.INVOICE_ID,
        AID.dist_code_combination_id,
        AID.project_id,
        AID.TASK_ID,
        AID.PO_DISTRIBUTION_ID
    ) AS INV
    WHERE
      1 = 1 AND AIP.INVOICE_ID = INV.INVOICE_ID AND aip.check_id = ac.check_id
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ap_cash_burn' AND batch_name = 'ap';