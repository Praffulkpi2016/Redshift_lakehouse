/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_CASH_BURN_SUMMARY
WHERE
  (COALESCE(CHECK_ID, 0), COALESCE(INVOICE_ID, 0), COALESCE(AE_LINE_NUM, 0), COALESCE(AE_HEADER_ID, 0), COALESCE(INVOICE_PAYMENT_ID, 0)) IN (
    SELECT
      COALESCE(ods.CHECK_ID, 0) AS CHECK_ID,
      COALESCE(ods.INVOICE_ID, 0) AS INVOICE_ID,
      COALESCE(ods.AE_LINE_NUM, 0) AS AE_LINE_NUM,
      COALESCE(ods.AE_HEADER_ID, 0) AS AE_HEADER_ID,
      COALESCE(ods.INVOICE_PAYMENT_ID, 0) AS INVOICE_PAYMENT_ID
    FROM gold_bec_dwh.fact_cash_burn_summary AS dw, (
      SELECT DISTINCT
        ac.check_number,
        ac.check_date,
        ac.vendor_name,
        aip.amount AS Amount,
        ac.check_id,
        aip.invoice_id,
        ac.status_lookup_code,
        xel.code_combination_id,
        xel.ae_line_num,
        xel.ae_header_id,
        xte.entity_id,
        xeh.event_id,
        aip.INVOICE_PAYMENT_ID
      FROM BEC_ODS.AP_CHECKS_ALL AS ac, BEC_ODS.AP_INVOICE_PAYMENTS_ALL AS AIP, BEC_ODS.xla_ae_lines AS xel, BEC_ODS.xla_ae_headers AS xeh, BEC_ODS.xla_transaction_entities AS xte
      WHERE
        1 = 1
        AND AIP.check_id = AC.check_id
        AND aip.set_of_books_id = xel.ledger_id
        AND xel.application_id = xeh.application_id
        AND xte.application_id = xeh.application_id
        AND xel.ae_header_id = xeh.ae_header_id
        AND xte.source_id_int_1 = aip.check_id
        AND aip.ACCOUNTING_EVENT_ID = xeh.event_id
        AND xte.entity_id = xeh.entity_id
        AND xte.entity_code = 'AP_PAYMENTS'
        AND XEL.ACCOUNTING_CLASS_CODE IN ('CASH', 'CASH_CLEARING')
        AND xte.application_id = 200
        AND (
          ac.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_cash_burn_summary' AND batch_name = 'ap'
          )
          OR aip.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_cash_burn_summary' AND batch_name = 'ap'
          )
          OR xeh.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_cash_burn_summary' AND batch_name = 'ap'
          )
          OR xte.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_cash_burn_summary' AND batch_name = 'ap'
          )
          OR xel.kca_seq_date >= (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_cash_burn_summary' AND batch_name = 'ap'
          )
          OR ac.is_deleted_flg = 'Y'
          OR aip.is_deleted_flg = 'Y'
          OR xeh.is_deleted_flg = 'Y'
          OR xte.is_deleted_flg = 'Y'
          OR xel.is_deleted_flg = 'Y'
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ODS.CHECK_ID, 0) || '-' || COALESCE(ODS.invoice_id, 0) || '-' || COALESCE(ODS.ae_line_num, 0) || '-' || COALESCE(ODS.ae_header_id, 0) || '-' || COALESCE(ODS.INVOICE_PAYMENT_ID, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.fact_cash_burn_summary (
  ORG_ID,
  check_number,
  check_date,
  vendor_name,
  amount,
  check_id,
  invoice_id,
  status_lookup_code,
  code_combination_id,
  ae_line_num,
  ae_header_id,
  entity_id,
  event_id,
  invoice_payment_id,
  check_id_key,
  invoice_id_key,
  ae_header_id_key,
  code_combination_id_key,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT DISTINCT
    AC.ORG_ID,
    ac.check_number,
    ac.check_date,
    ac.vendor_name,
    aip.amount AS Amount,
    ac.check_id,
    aip.invoice_id,
    ac.status_lookup_code,
    xel.code_combination_id,
    xel.ae_line_num,
    xel.ae_header_id,
    xte.entity_id,
    xeh.event_id,
    aip.INVOICE_PAYMENT_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ac.check_id AS CHECK_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || aip.invoice_id AS INVOICE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || xel.ae_header_id AS AE_HEADER_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || xel.code_combination_id AS CODE_COMBINATION_ID_KEY,
    'N' AS is_deleted_flg,
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
    ) || '-' || COALESCE(AC.CHECK_ID, 0) || '-' || COALESCE(AIP.invoice_id, 0) || '-' || COALESCE(xel.ae_line_num, 0) || '-' || COALESCE(xel.ae_header_id, 0) || '-' || COALESCE(aip.INVOICE_PAYMENT_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM BEC_ODS.AP_CHECKS_ALL
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS ac, (
    SELECT
      *
    FROM BEC_ODS.AP_INVOICE_PAYMENTS_ALL
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS AIP, (
    SELECT
      *
    FROM BEC_ODS.xla_ae_lines
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS xel, (
    SELECT
      *
    FROM BEC_ODS.xla_ae_headers
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS xeh, (
    SELECT
      *
    FROM BEC_ODS.xla_transaction_entities
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS xte
  WHERE
    1 = 1
    AND AIP.check_id = AC.check_id
    AND aip.set_of_books_id = xel.ledger_id
    AND xel.application_id = xeh.application_id
    AND xte.application_id = xeh.application_id
    AND xel.ae_header_id = xeh.ae_header_id
    AND xte.source_id_int_1 = aip.check_id
    AND aip.ACCOUNTING_EVENT_ID = xeh.event_id
    AND xte.entity_id = xeh.entity_id
    AND xte.entity_code = 'AP_PAYMENTS'
    AND XEL.ACCOUNTING_CLASS_CODE IN ('CASH', 'CASH_CLEARING')
    AND xte.application_id = 200
    AND (
      ac.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_cash_burn_summary' AND batch_name = 'ap'
      )
      OR aip.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_cash_burn_summary' AND batch_name = 'ap'
      )
      OR xeh.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_cash_burn_summary' AND batch_name = 'ap'
      )
      OR xte.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_cash_burn_summary' AND batch_name = 'ap'
      )
      OR xel.kca_seq_date >= (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_cash_burn_summary' AND batch_name = 'ap'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_cash_burn_summary' AND batch_name = 'ap';