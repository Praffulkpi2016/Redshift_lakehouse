DROP table IF EXISTS gold_bec_dwh.FACT_XLA_REPORT_BALANCES_STG;
CREATE TABLE gold_bec_dwh.FACT_XLA_REPORT_BALANCES_STG AS
SELECT
  LEDGER_ID,
  LEDGER_SHORT_NAME,
  LEDGER_DESCRIPTION,
  LEDGER_NAME,
  LEDGER_CURRENCY,
  PERIOD_YEAR,
  PERIOD_NUMBER,
  PERIOD_NAME,
  PERIOD_START_DATE,
  PERIOD_END_DATE,
  BALANCE_TYPE_CODE,
  BALANCE_TYPE,
  BUDGET_VERSION_ID,
  BUDGET_NAME,
  ENCUMBRANCE_TYPE_ID,
  ENCUMBRANCE_TYPE,
  BEGIN_BALANCE_DR,
  BEGIN_BALANCE_CR,
  PERIOD_NET_DR,
  PERIOD_NET_CR,
  CODE_COMBINATION_ID,
  CONTROL_ACCOUNT_FLAG,
  CONTROL_ACCOUNT,
  SEGMENT1,
  SEGMENT2,
  SEGMENT3,
  SEGMENT4,
  SEGMENT5,
  SEGMENT6,
  SEGMENT7,
  SEGMENT8,
  SEGMENT9,
  SEGMENT10,
  SEGMENT11,
  SEGMENT12,
  SEGMENT13,
  SEGMENT14,
  SEGMENT15,
  SEGMENT16,
  SEGMENT17,
  SEGMENT18,
  SEGMENT19,
  SEGMENT20,
  SEGMENT21,
  SEGMENT22,
  SEGMENT23,
  SEGMENT24,
  SEGMENT25,
  SEGMENT26,
  SEGMENT27,
  SEGMENT28,
  SEGMENT29,
  SEGMENT30,
  TRANSLATED_FLAG,
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
  ) || '-' || COALESCE(LEDGER_ID, 0) || '-' || COALESCE(BUDGET_NAME, 'NA') || '-' || COALESCE(PERIOD_NAME, 'NA') || '-' || COALESCE(CODE_COMBINATION_ID, 0) || '-' || COALESCE(LEDGER_CURRENCY, 'NA') || '-' || COALESCE(TRANSLATED_FLAG, 'NA') AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    gl1.ledger_id AS LEDGER_ID,
    gl1.short_name AS LEDGER_SHORT_NAME,
    gl1.description AS LEDGER_DESCRIPTION,
    gl1.NAME AS LEDGER_NAME,
    glb.currency_code AS LEDGER_CURRENCY,
    glb.period_year AS PERIOD_YEAR,
    glb.period_num AS PERIOD_NUMBER,
    glb.period_name AS PERIOD_NAME,
    gl1.START_DATE AS PERIOD_START_DATE,
    gl1.end_date AS PERIOD_END_DATE,
    glb.actual_flag AS BALANCE_TYPE_CODE,
    xlk.meaning AS BALANCE_TYPE,
    glb.budget_version_id AS BUDGET_VERSION_ID,
    glv.budget_name AS BUDGET_NAME,
    glb.encumbrance_type_id AS ENCUMBRANCE_TYPE_ID,
    get.encumbrance_type AS ENCUMBRANCE_TYPE,
    COALESCE(glb.begin_balance_dr, 0) AS BEGIN_BALANCE_DR,
    COALESCE(glb.begin_balance_cr, 0) AS BEGIN_BALANCE_CR,
    COALESCE(glb.period_net_dr, 0) AS PERIOD_NET_DR,
    COALESCE(glb.period_net_cr, 0) AS PERIOD_NET_CR,
    glb.code_combination_id AS CODE_COMBINATION_ID, /*           ,xla_report_utility_pkg.get_ccid_desc */ /*              (gl1.chart_of_accounts_id */ /*              ,glb.code_combination_id)   CODE_COMBINATION_DESCRIPTION */
    gcck.reference3 AS CONTROL_ACCOUNT_FLAG,
    NULL AS CONTROL_ACCOUNT,
    gcck.segment1 AS SEGMENT1,
    gcck.segment2 AS SEGMENT2,
    gcck.segment3 AS SEGMENT3,
    gcck.segment4 AS SEGMENT4,
    gcck.segment5 AS SEGMENT5,
    gcck.segment6 AS SEGMENT6,
    gcck.segment7 AS SEGMENT7,
    gcck.segment8 AS SEGMENT8,
    gcck.segment9 AS SEGMENT9,
    gcck.segment10 AS SEGMENT10,
    gcck.segment11 AS SEGMENT11,
    gcck.segment12 AS SEGMENT12,
    gcck.segment13 AS SEGMENT13,
    gcck.segment14 AS SEGMENT14,
    gcck.segment15 AS SEGMENT15,
    gcck.segment16 AS SEGMENT16,
    gcck.segment17 AS SEGMENT17,
    gcck.segment18 AS SEGMENT18,
    gcck.segment19 AS SEGMENT19,
    gcck.segment20 AS SEGMENT20,
    gcck.segment21 AS SEGMENT21,
    gcck.segment22 AS SEGMENT22,
    gcck.segment23 AS SEGMENT23,
    gcck.segment24 AS SEGMENT24,
    gcck.segment25 AS SEGMENT25,
    gcck.segment26 AS SEGMENT26,
    gcck.segment27 AS SEGMENT27,
    gcck.segment28 AS SEGMENT28,
    gcck.segment29 AS SEGMENT29,
    gcck.segment30 AS SEGMENT30,
    glb.translated_flag AS TRANSLATED_FLAG
  FROM (
    SELECT
      gll.ledger_id,
      gll.short_name,
      gll.description,
      gll.name,
      gll.currency_code,
      gll.chart_of_accounts_id,
      gls.period_name,
      gls.start_date,
      gls.end_date
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_ledgers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS gll, (
      SELECT
        *
      FROM silver_bec_ods.gl_period_statuses
      WHERE
        is_deleted_flg <> 'Y'
    ) AS gls
    WHERE
      gls.ledger_id = gll.ledger_id AND gls.application_id = 101
  ) AS gl1, (
    SELECT
      *
    FROM silver_bec_ods.gl_balances
    WHERE
      is_deleted_flg <> 'Y'
  ) AS glb, (
    SELECT
      *
    FROM silver_bec_ods.gl_code_combinations
    WHERE
      is_deleted_flg <> 'Y'
  ) AS gcck, (
    SELECT
      *
    FROM silver_bec_ods.fnd_lookup_values
    WHERE
      is_deleted_flg <> 'Y'
  ) AS xlk, (
    SELECT
      *
    FROM silver_bec_ods.gl_budget_versions
    WHERE
      is_deleted_flg <> 'Y'
  ) AS glv, (
    SELECT
      *
    FROM silver_bec_ods.gl_encumbrance_types
    WHERE
      is_deleted_flg <> 'Y'
  ) AS get
  WHERE
    glb.ledger_id = gl1.ledger_id
    AND glb.period_name = gl1.period_name
    AND glb.template_id IS NULL
    AND gcck.code_combination_id = glb.code_combination_id
    AND gcck.chart_of_accounts_id = gl1.chart_of_accounts_id
    AND xlk.lookup_type = 'XLA_BALANCE_TYPE'
    AND xlk.lookup_code = glb.actual_flag
    AND glv.BUDGET_VERSION_ID() = glb.budget_version_id
    AND get.ENCUMBRANCE_TYPE_ID() = glb.encumbrance_type_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_xla_report_balances_stg' AND batch_name = 'gl';