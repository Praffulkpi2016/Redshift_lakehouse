DROP table IF EXISTS gold_bec_dwh.FACT_CASH_BURN_SUMMARY;
CREATE TABLE gold_bec_dwh.FACT_CASH_BURN_SUMMARY AS
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
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_cash_burn_summary' AND batch_name = 'ap';