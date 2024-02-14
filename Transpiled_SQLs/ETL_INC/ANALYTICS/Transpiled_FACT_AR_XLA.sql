/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_AR_XLA
WHERE
  (COALESCE(ENTITY_ID, 0), COALESCE(AE_HEADER_ID, 0), COALESCE(AE_LINE_NUM, 0), COALESCE(LINE_ID, 0), COALESCE(CODE_COMBINATION_ID, 0), COALESCE(SOURCE_DISTRIBUTION_ID_NUM_1, 0)) IN (
    SELECT
      COALESCE(ods.ENTITY_ID, 0) AS ENTITY_ID,
      COALESCE(ods.AE_HEADER_ID, 0) AS AE_HEADER_ID,
      COALESCE(ods.AE_LINE_NUM, 0) AS AE_LINE_NUM,
      COALESCE(ods.LINE_ID, 0) AS LINE_ID,
      COALESCE(ods.CODE_COMBINATION_ID, 0) AS CODE_COMBINATION_ID,
      COALESCE(ods.SOURCE_DISTRIBUTION_ID_NUM_1, 0) AS SOURCE_DISTRIBUTION_ID_NUM_1
    FROM gold_bec_dwh.FACT_AR_XLA AS dw, (
      SELECT
        XTE.ENTITY_ID AS ENTITY_ID,
        XAH.AE_HEADER_ID AS AE_HEADER_ID,
        XAL.AE_LINE_NUM AS AE_LINE_NUM,
        ADA.LINE_ID AS LINE_ID,
        XAL.CODE_COMBINATION_ID AS CODE_COMBINATION_ID,
        XDL.SOURCE_DISTRIBUTION_ID_NUM_1 AS SOURCE_DISTRIBUTION_ID_NUM_1,
        XAH.kca_seq_date
      FROM silver_bec_ods.XLA_TRANSACTION_ENTITIES AS XTE, silver_bec_ods.XLA_AE_HEADERS AS XAH, silver_bec_ods.XLA_AE_LINES AS XAL, silver_bec_ods.XLA_DISTRIBUTION_LINKS AS XDL, silver_bec_ods.ar_distributions_all AS ada
      WHERE
        XTE.ENTITY_ID = XAH.ENTITY_ID
        AND XAH.AE_HEADER_ID = XAL.AE_HEADER_ID
        AND XAH.AE_HEADER_ID = XDL.AE_HEADER_ID
        AND XAL.ae_line_num = XDL.ae_line_num
        AND XDL.SOURCE_DISTRIBUTION_ID_NUM_1 = ADA.source_id
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.ENTITY_ID, 0) || '-' || COALESCE(ods.AE_HEADER_ID, 0) || '-' || COALESCE(ods.AE_LINE_NUM, 0) || '-' || COALESCE(ods.LINE_ID, 0) || '-' || COALESCE(ods.CODE_COMBINATION_ID, 0) || '-' || COALESCE(ods.SOURCE_DISTRIBUTION_ID_NUM_1, 0)
      AND ods.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_ar_xla' AND batch_name = 'ar'
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_AR_XLA (
  xla_dist_type,
  entity_id,
  entity_id_key,
  application_id,
  application_id_key,
  legal_entity_id,
  legal_entity_id_key,
  entity_code,
  source_id_int_1,
  transaction_number,
  valuation_method,
  source_application_id,
  security_id_int_1,
  event_type_code,
  event_date,
  event_status_code,
  process_status_code,
  on_hold_flag,
  transaction_date,
  ae_header_id,
  ledger_id,
  ledger_id_key,
  header_accounting_date,
  gl_transfer_status_code,
  gl_transfer_date,
  je_category_name,
  accounting_entry_status_code,
  accounting_entry_type_code,
  header_description,
  budget_version_id,
  balance_type_code,
  period_name,
  ae_line_num,
  code_combination_id,
  gl_sl_link_id,
  accounting_class_code,
  entered_dr,
  entered_cr,
  accounted_dr,
  accounted_cr,
  sla_line_description,
  currency_code,
  gl_sl_link_table,
  line_unrounded_accounted_dr,
  line_unrounded_accounted_cr,
  line_unrounded_entered_dr,
  line_unrounded_entered_cr,
  line_accounting_date,
  source_table,
  source_distribution_type,
  source_distribution_id_num_1,
  event_class_code,
  dist_unrounded_entered_dr,
  dist_unrounded_entered_cr,
  dist_unrounded_accounted_dr,
  dist_unrounded_accounted_cr,
  applied_to_source_id_num_1,
  applied_to_distribution_type,
  applied_to_dist_id_num_1,
  source_id,
  line_id,
  line_id_key,
  org_id,
  org_id_key,
  amount_dr,
  amount_cr,
  acctd_amount_dr,
  acctd_amount_cr,
  customer_trx_id,
  customer_trx_id_key,
  customer_trx_line_id,
  customer_trx_line_id_key,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date,
  last_update_date
)
(
  SELECT
    XLA_DIST_TYPE,
    ENTITY_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ENTITY_ID AS ENTITY_ID_KEY,
    APPLICATION_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || APPLICATION_ID AS APPLICATION_ID_KEY,
    LEGAL_ENTITY_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || LEGAL_ENTITY_ID AS LEGAL_ENTITY_ID_KEY,
    ENTITY_CODE,
    SOURCE_ID_INT_1,
    TRANSACTION_NUMBER,
    VALUATION_METHOD,
    SOURCE_APPLICATION_ID,
    SECURITY_ID_INT_1,
    EVENT_TYPE_CODE,
    EVENT_DATE,
    EVENT_STATUS_CODE,
    PROCESS_STATUS_CODE,
    ON_HOLD_FLAG,
    TRANSACTION_DATE,
    AE_HEADER_ID,
    LEDGER_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || LEDGER_ID AS LEDGER_ID_KEY,
    HEADER_ACCOUNTING_DATE,
    GL_TRANSFER_STATUS_CODE,
    GL_TRANSFER_DATE,
    JE_CATEGORY_NAME,
    ACCOUNTING_ENTRY_STATUS_CODE,
    ACCOUNTING_ENTRY_TYPE_CODE,
    HEADER_DESCRIPTION,
    BUDGET_VERSION_ID,
    BALANCE_TYPE_CODE,
    PERIOD_NAME,
    AE_LINE_NUM,
    CODE_COMBINATION_ID,
    GL_SL_LINK_ID,
    ACCOUNTING_CLASS_CODE,
    ENTERED_DR,
    ENTERED_CR,
    ACCOUNTED_DR,
    ACCOUNTED_CR,
    SLA_LINE_DESCRIPTION,
    CURRENCY_CODE,
    GL_SL_LINK_TABLE,
    LINE_UNROUNDED_ACCOUNTED_DR,
    LINE_UNROUNDED_ACCOUNTED_CR,
    LINE_UNROUNDED_ENTERED_DR,
    LINE_UNROUNDED_ENTERED_CR,
    LINE_ACCOUNTING_DATE,
    SOURCE_TABLE,
    SOURCE_DISTRIBUTION_TYPE,
    SOURCE_DISTRIBUTION_ID_NUM_1,
    EVENT_CLASS_CODE,
    DIST_UNROUNDED_ENTERED_DR,
    DIST_UNROUNDED_ENTERED_CR,
    DIST_UNROUNDED_ACCOUNTED_DR,
    DIST_UNROUNDED_ACCOUNTED_CR,
    APPLIED_TO_SOURCE_ID_NUM_1,
    APPLIED_TO_DISTRIBUTION_TYPE,
    APPLIED_TO_DIST_ID_NUM_1,
    source_id,
    line_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || line_id AS line_id_key,
    ORG_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ORG_ID AS ORG_ID_key,
    AMOUNT_DR,
    AMOUNT_CR,
    ACCTD_AMOUNT_DR,
    ACCTD_AMOUNT_CR,
    CUSTOMER_TRX_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || CUSTOMER_TRX_ID AS CUSTOMER_TRX_ID_KEY,
    CUSTOMER_TRX_LINE_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || CUSTOMER_TRX_LINE_ID AS CUSTOMER_TRX_LINE_ID_KEY,
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
    ) || '-' || COALESCE(ENTITY_ID, 0) || '-' || COALESCE(AE_HEADER_ID, 0) || '-' || COALESCE(AE_LINE_NUM, 0) || '-' || COALESCE(LINE_ID, 0) || '-' || COALESCE(CODE_COMBINATION_ID, 0) || '-' || COALESCE(SOURCE_DISTRIBUTION_ID_NUM_1, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date,
    LAST_UPDATE_DATE
  FROM (
    SELECT
      'XLA_TE' AS XLA_DIST_TYPE,
      XTE.ENTITY_ID,
      XTE.APPLICATION_ID,
      XTE.LEGAL_ENTITY_ID,
      XTE.ENTITY_CODE,
      XTE.SOURCE_ID_INT_1,
      XTE.TRANSACTION_NUMBER,
      XTE.VALUATION_METHOD,
      XTE.SOURCE_APPLICATION_ID,
      XTE.SECURITY_ID_INT_1,
      XE.EVENT_TYPE_CODE,
      XE.EVENT_DATE,
      XE.EVENT_STATUS_CODE,
      XE.PROCESS_STATUS_CODE,
      XE.ON_HOLD_FLAG,
      XE.TRANSACTION_DATE,
      XAH.AE_HEADER_ID,
      XAH.LEDGER_ID,
      XAH.ACCOUNTING_DATE AS HEADER_ACCOUNTING_DATE,
      XAH.GL_TRANSFER_STATUS_CODE,
      XAH.GL_TRANSFER_DATE,
      XAH.JE_CATEGORY_NAME,
      XAH.ACCOUNTING_ENTRY_STATUS_CODE,
      XAH.ACCOUNTING_ENTRY_TYPE_CODE,
      XAH.DESCRIPTION AS HEADER_DESCRIPTION,
      XAH.BUDGET_VERSION_ID,
      XAH.BALANCE_TYPE_CODE,
      XAH.PERIOD_NAME,
      XAL.AE_LINE_NUM,
      XAL.CODE_COMBINATION_ID,
      XAL.GL_SL_LINK_ID,
      XAL.ACCOUNTING_CLASS_CODE,
      XAL.ENTERED_DR,
      XAL.ENTERED_CR,
      XAL.ACCOUNTED_DR,
      XAL.ACCOUNTED_CR,
      XAL.DESCRIPTION AS SLA_LINE_DESCRIPTION,
      XAL.CURRENCY_CODE,
      XAL.GL_SL_LINK_TABLE,
      COALESCE(XAL.UNROUNDED_ACCOUNTED_DR, 0) AS LINE_UNROUNDED_ACCOUNTED_DR,
      COALESCE(XAL.UNROUNDED_ACCOUNTED_CR, 0) AS LINE_UNROUNDED_ACCOUNTED_CR,
      COALESCE(XAL.UNROUNDED_ENTERED_DR, 0) AS LINE_UNROUNDED_ENTERED_DR,
      COALESCE(XAL.UNROUNDED_ENTERED_CR, 0) AS LINE_UNROUNDED_ENTERED_CR,
      XAL.ACCOUNTING_DATE AS LINE_ACCOUNTING_DATE,
      XAL.SOURCE_TABLE,
      NULL AS SOURCE_DISTRIBUTION_TYPE,
      NULL AS SOURCE_DISTRIBUTION_ID_NUM_1,
      NULL AS EVENT_CLASS_CODE,
      NULL AS DIST_UNROUNDED_ENTERED_DR,
      NULL AS DIST_UNROUNDED_ENTERED_CR,
      NULL AS DIST_UNROUNDED_ACCOUNTED_DR,
      NULL AS DIST_UNROUNDED_ACCOUNTED_CR,
      NULL AS APPLIED_TO_SOURCE_ID_NUM_1,
      NULL AS APPLIED_TO_DISTRIBUTION_TYPE,
      NULL AS APPLIED_TO_DIST_ID_NUM_1,
      NULL AS source_id,
      NULL AS line_id,
      SECURITY_ID_INT_1 AS ORG_ID,
      NULL AS AMOUNT_DR,
      NULL AS AMOUNT_CR,
      NULL AS ACCTD_AMOUNT_DR,
      NULL AS ACCTD_AMOUNT_CR,
      SOURCE_ID_INT_1 AS CUSTOMER_TRX_ID,
      NULL AS CUSTOMER_TRX_LINE_ID,
      XAH.LAST_UPDATE_DATE
    FROM silver_bec_ods.XLA_TRANSACTION_ENTITIES AS XTE, silver_bec_ods.XLA_AE_HEADERS AS XAH, silver_bec_ods.XLA_AE_LINES AS XAL, silver_bec_ods.XLA_EVENTS AS XE
    WHERE
      1 = 1
      AND XTE.APPLICATION_ID = 222 /* -200 */
      AND XTE.ENTITY_ID = XAH.ENTITY_ID
      AND COALESCE(XE.event_id, 0) = COALESCE(XAH.event_id, 0)
      AND XAH.AE_HEADER_ID = XAL.AE_HEADER_ID
    UNION ALL
    SELECT
      'XLA_ARD' AS XLA_DIST_TYPE,
      NULL AS ENTITY_ID,
      XDL.APPLICATION_ID,
      NULL AS LEGAL_ENTITY_ID,
      NULL AS ENTITY_CODE,
      NULL AS SOURCE_ID_INT_1,
      NULL AS TRANSACTION_NUMBER,
      NULL AS VALUATION_METHOD,
      NULL AS SOURCE_APPLICATION_ID,
      NULL AS SECURITY_ID_INT_1,
      XDL.EVENT_TYPE_CODE,
      NULL AS EVENT_DATE,
      NULL AS EVENT_STATUS_CODE,
      NULL AS PROCESS_STATUS_CODE,
      NULL AS ON_HOLD_FLAG,
      NULL AS TRANSACTION_DATE,
      XAH.AE_HEADER_ID,
      XAH.LEDGER_ID,
      XAH.ACCOUNTING_DATE AS HEADER_ACCOUNTING_DATE,
      XAH.GL_TRANSFER_STATUS_CODE,
      XAH.GL_TRANSFER_DATE,
      XAH.JE_CATEGORY_NAME,
      XAH.ACCOUNTING_ENTRY_STATUS_CODE,
      XAH.ACCOUNTING_ENTRY_TYPE_CODE,
      XAH.DESCRIPTION AS HEADER_DESCRIPTION,
      XAH.BUDGET_VERSION_ID,
      XAH.BALANCE_TYPE_CODE,
      XAH.PERIOD_NAME,
      XAL.AE_LINE_NUM,
      XAL.CODE_COMBINATION_ID,
      XAL.GL_SL_LINK_ID,
      XAL.ACCOUNTING_CLASS_CODE,
      XAL.ENTERED_DR,
      XAL.ENTERED_CR,
      XAL.ACCOUNTED_DR,
      XAL.ACCOUNTED_CR,
      XAL.DESCRIPTION AS SLA_LINE_DESCRIPTION,
      XAL.CURRENCY_CODE,
      XAL.GL_SL_LINK_TABLE,
      COALESCE(XAL.UNROUNDED_ACCOUNTED_DR, 0) AS LINE_UNROUNDED_ACCOUNTED_DR,
      COALESCE(XAL.UNROUNDED_ACCOUNTED_CR, 0) AS LINE_UNROUNDED_ACCOUNTED_CR,
      COALESCE(XAL.UNROUNDED_ENTERED_DR, 0) AS LINE_UNROUNDED_ENTERED_DR,
      COALESCE(XAL.UNROUNDED_ENTERED_CR, 0) AS LINE_UNROUNDED_ENTERED_CR,
      XAL.ACCOUNTING_DATE AS LINE_ACCOUNTING_DATE,
      XAL.SOURCE_TABLE,
      XDL.SOURCE_DISTRIBUTION_TYPE,
      XDL.SOURCE_DISTRIBUTION_ID_NUM_1,
      XDL.EVENT_CLASS_CODE,
      COALESCE(XDL.UNROUNDED_ENTERED_DR, 0) AS DIST_UNROUNDED_ENTERED_DR,
      COALESCE(XDL.UNROUNDED_ENTERED_CR, 0) AS DIST_UNROUNDED_ENTERED_CR,
      COALESCE(XDL.UNROUNDED_ACCOUNTED_CR, 0) AS DIST_UNROUNDED_ACCOUNTED_DR,
      COALESCE(XDL.UNROUNDED_ACCOUNTED_DR, 0) AS DIST_UNROUNDED_ACCOUNTED_CR,
      XDL.APPLIED_TO_SOURCE_ID_NUM_1,
      XDL.APPLIED_TO_DISTRIBUTION_TYPE,
      XDL.APPLIED_TO_DIST_ID_NUM_1,
      ADA.source_id,
      ADA.line_id,
      ADA.org_id,
      ADA.AMOUNT_DR,
      ADA.AMOUNT_CR,
      ADA.ACCTD_AMOUNT_DR,
      ADA.ACCTD_AMOUNT_CR,
      NULL AS CUSTOMER_TRX_ID,
      NULL AS CUSTOMER_TRX_LINE_ID,
      XAH.LAST_UPDATE_DATE
    FROM silver_bec_ods.XLA_AE_HEADERS AS XAH, silver_bec_ods.XLA_AE_LINES AS XAL, silver_bec_ods.XLA_DISTRIBUTION_LINKS AS XDL, silver_bec_ods.ar_distributions_all AS ADA
    WHERE
      1 = 1
      AND XDL.APPLICATION_ID = 222
      AND XAL.AE_HEADER_ID = XAH.AE_HEADER_ID
      AND XAH.AE_HEADER_ID = XDL.AE_HEADER_ID
      AND XAL.ae_line_num = XDL.ae_line_num /* added join condition to handle duplicates */
      AND XDL.SOURCE_DISTRIBUTION_ID_NUM_1 = ADA.source_id
      AND XDL.SOURCE_DISTRIBUTION_TYPE = 'AR_DISTRIBUTIONS_ALL'
    UNION ALL
    SELECT
      'XLA_DL' AS XLA_DIST_TYPE,
      NULL AS ENTITY_ID,
      XDL.APPLICATION_ID,
      NULL AS LEGAL_ENTITY_ID,
      NULL AS ENTITY_CODE,
      NULL AS SOURCE_ID_INT_1,
      NULL AS TRANSACTION_NUMBER,
      NULL AS VALUATION_METHOD,
      NULL AS SOURCE_APPLICATION_ID,
      NULL AS SECURITY_ID_INT_1,
      XDL.EVENT_TYPE_CODE,
      NULL AS EVENT_DATE,
      NULL AS EVENT_STATUS_CODE,
      NULL AS PROCESS_STATUS_CODE,
      NULL AS ON_HOLD_FLAG,
      NULL AS TRANSACTION_DATE,
      XAH.AE_HEADER_ID,
      XAH.LEDGER_ID,
      XAH.ACCOUNTING_DATE AS HEADER_ACCOUNTING_DATE,
      XAH.GL_TRANSFER_STATUS_CODE,
      XAH.GL_TRANSFER_DATE,
      XAH.JE_CATEGORY_NAME,
      XAH.ACCOUNTING_ENTRY_STATUS_CODE,
      XAH.ACCOUNTING_ENTRY_TYPE_CODE,
      XAH.DESCRIPTION AS HEADER_DESCRIPTION,
      XAH.BUDGET_VERSION_ID,
      XAH.BALANCE_TYPE_CODE,
      XAH.PERIOD_NAME,
      XAL.AE_LINE_NUM,
      XAL.CODE_COMBINATION_ID,
      XAL.GL_SL_LINK_ID,
      XAL.ACCOUNTING_CLASS_CODE,
      XAL.ENTERED_DR,
      XAL.ENTERED_CR,
      XAL.ACCOUNTED_DR,
      XAL.ACCOUNTED_CR,
      XAL.DESCRIPTION AS SLA_LINE_DESCRIPTION,
      XAL.CURRENCY_CODE,
      XAL.GL_SL_LINK_TABLE,
      COALESCE(XAL.UNROUNDED_ACCOUNTED_DR, 0) AS LINE_UNROUNDED_ACCOUNTED_DR,
      COALESCE(XAL.UNROUNDED_ACCOUNTED_CR, 0) AS LINE_UNROUNDED_ACCOUNTED_CR,
      COALESCE(XAL.UNROUNDED_ENTERED_DR, 0) AS LINE_UNROUNDED_ENTERED_DR,
      COALESCE(XAL.UNROUNDED_ENTERED_CR, 0) AS LINE_UNROUNDED_ENTERED_CR,
      XAL.ACCOUNTING_DATE AS LINE_ACCOUNTING_DATE,
      XAL.SOURCE_TABLE,
      XDL.SOURCE_DISTRIBUTION_TYPE,
      XDL.SOURCE_DISTRIBUTION_ID_NUM_1,
      XDL.EVENT_CLASS_CODE,
      COALESCE(XDL.UNROUNDED_ENTERED_DR, 0) AS DIST_UNROUNDED_ENTERED_DR,
      COALESCE(XDL.UNROUNDED_ENTERED_CR, 0) AS DIST_UNROUNDED_ENTERED_CR,
      COALESCE(XDL.UNROUNDED_ACCOUNTED_CR, 0) AS DIST_UNROUNDED_ACCOUNTED_DR,
      COALESCE(XDL.UNROUNDED_ACCOUNTED_DR, 0) AS DIST_UNROUNDED_ACCOUNTED_CR,
      XDL.APPLIED_TO_SOURCE_ID_NUM_1,
      XDL.APPLIED_TO_DISTRIBUTION_TYPE,
      XDL.APPLIED_TO_DIST_ID_NUM_1,
      NULL AS source_id,
      NULL AS line_id,
      RTGDA.org_id,
      NULL AS AMOUNT_DR,
      NULL AS AMOUNT_CR,
      NULL AS ACCTD_AMOUNT_DR,
      NULL AS ACCTD_AMOUNT_CR,
      RTGDA.CUSTOMER_TRX_ID,
      RTGDA.CUSTOMER_TRX_LINE_ID,
      XAH.LAST_UPDATE_DATE
    FROM silver_bec_ods.XLA_AE_HEADERS AS XAH, silver_bec_ods.XLA_AE_LINES AS XAL, silver_bec_ods.XLA_DISTRIBUTION_LINKS AS XDL, silver_bec_ods.RA_CUST_TRX_LINE_GL_DIST_ALL AS RTGDA
    WHERE
      1 = 1
      AND XDL.APPLICATION_ID = 222
      AND XAL.AE_HEADER_ID = XAH.AE_HEADER_ID
      AND XAH.AE_HEADER_ID = XDL.AE_HEADER_ID
      AND XAL.ae_line_num = XDL.ae_line_num /* added join condition to handle duplicates */
      AND XDL.SOURCE_DISTRIBUTION_ID_NUM_1 = RTGDA.CUST_TRX_LINE_GL_DIST_ID
      AND XDL.SOURCE_DISTRIBUTION_TYPE = 'RA_CUST_TRX_LINE_GL_DIST_ALL'
      AND XAH.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_ar_xla' AND batch_name = 'ar'
      )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ar_xla' AND batch_name = 'ar';