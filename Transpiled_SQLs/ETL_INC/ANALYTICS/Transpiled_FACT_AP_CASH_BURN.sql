/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_AP_CASH_BURN
WHERE
  (
    (COALESCE(INVOICE_ID, 0), COALESCE(dist_code_combination_id, 0), COALESCE(PO_DISTRIBUTION_ID, 0), COALESCE(INVOICE_PAYMENT_ID, 0), COALESCE(project_id, 0), COALESCE(task_id, 0))
  ) IN (
    SELECT
      COALESCE(ODS.INVOICE_ID, 0) AS INVOICE_ID,
      COALESCE(ODS.dist_code_combination_id, 0) AS dist_code_combination_id,
      COALESCE(ODS.PO_DISTRIBUTION_ID, 0) AS PO_DISTRIBUTION_ID,
      COALESCE(ODS.INVOICE_PAYMENT_ID, 0) AS INVOICE_PAYMENT_ID,
      COALESCE(ODS.project_id, 0) AS project_id,
      COALESCE(ODS.task_id, 0) AS task_id
    FROM gold_bec_dwh.FACT_AP_CASH_BURN AS dw, (
      SELECT
        aip.invoice_id,
        aip.INVOICE_PAYMENT_ID,
        inv.PO_DISTRIBUTION_ID,
        inv.dist_code_combination_id,
        inv.project_id,
        inv.task_id
      FROM silver_bec_ods.AP_CHECKS_ALL AS ac, silver_bec_ods.AP_INVOICE_PAYMENTS_ALL AS AIP, (
        SELECT
          AIA.INVOICE_ID,
          AID.dist_code_combination_id,
          SUM(AID.AMOUNT) AS distribution_amount,
          AID.project_id,
          AID.TASK_ID,
          AID.PO_DISTRIBUTION_ID,
          AIA.is_deleted_flg,
          AILA.is_deleted_flg AS is_deleted_flg1,
          AID.is_deleted_flg AS is_deleted_flg2
        FROM silver_bec_ods.AP_INVOICES_ALL AS AIA, silver_bec_ods.AP_INVOICE_LINES_ALL AS AILA, silver_bec_ods.AP_INVOICE_DISTRIBUTIONS_ALL AS AID
        WHERE
          AIA.INVOICE_ID = AID.INVOICE_ID
          AND AIA.INVOICE_ID = AILA.INVOICE_ID
          AND AILA.LINE_NUMBER = AID.INVOICE_LINE_NUMBER
        GROUP BY
          AIA.INVOICE_ID,
          AID.dist_code_combination_id,
          AID.project_id,
          AID.TASK_ID,
          AID.PO_DISTRIBUTION_ID,
          AIA.is_deleted_flg,
          AILA.is_deleted_flg,
          AID.is_deleted_flg
      ) AS INV
      WHERE
        1 = 1
        AND AIP.INVOICE_ID = INV.INVOICE_ID
        AND aip.check_id = ac.check_id
        AND (
          aip.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_ap_cash_burn' AND batch_name = 'ap'
          )
          OR ac.is_deleted_flg = 'Y'
          OR AIP.is_deleted_flg = 'Y'
          OR INV.is_deleted_flg = 'Y'
          OR INV.is_deleted_flg1 = 'Y'
          OR INV.is_deleted_flg2 = 'Y'
        )
    ) AS ODS
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.INVOICE_ID, 0) || '-' || COALESCE(ods.dist_code_combination_id, 0) || '-' || COALESCE(ods.PO_DISTRIBUTION_ID, 0) || '-' || COALESCE(ods.INVOICE_PAYMENT_ID, 0) || '-' || COALESCE(ods.project_id, 0) || '-' || COALESCE(ods.task_id, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_AP_CASH_BURN (
  org_id,
  check_number,
  vendor_name,
  check_date,
  Amount,
  check_id,
  invoice_id,
  INVOICE_PAYMENT_ID,
  status_lookup_code,
  distribution_amount,
  PO_DISTRIBUTION_ID,
  dist_code_combination_id,
  project_id,
  task_id,
  check_id_key,
  invoice_id_key,
  INVOICE_PAYMENT_ID_key,
  PO_DISTRIBUTION_ID_key,
  dist_code_combination_id_key,
  project_id_key,
  task_id_key,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
      ac.org_id,
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
      1 = 1
      AND AIP.INVOICE_ID = INV.INVOICE_ID
      AND aip.check_id = ac.check_id
      AND (
        aip.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_ap_cash_burn' AND batch_name = 'ap'
        )
      )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ap_cash_burn' AND batch_name = 'ap';