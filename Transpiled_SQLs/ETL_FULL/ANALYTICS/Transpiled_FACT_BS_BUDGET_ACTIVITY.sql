DROP table IF EXISTS gold_bec_dwh.FACT_BS_BUDGET_ACTIVITY;
CREATE TABLE gold_bec_dwh.FACT_BS_BUDGET_ACTIVITY AS
(
  SELECT
    gbs.ledger_id AS ledger_id,
    gbs.chart_of_accounts_id AS chart_of_accounts_id,
    gbs.currency_code AS currency_code,
    gbs.code_combination_id AS code_combination_id,
    gbs.ledger_id || '-' || gbs.period_name AS period_name,
    gbs.period_name AS gl_period,
    gbs.period_year AS period_year,
    gbs.actual_flag,
    gbs.translated_flag,
    gbs.budget_version_id,
    gbs.account_type AS account_type,
    gbs.coa_segval3 AS coa_segval3,
    gbs.coa_segval2 AS coa_segval2,
    gbs.coa_segval1 AS coa_segval1,
    gbs.coa_segval4 AS coa_segval4,
    gbs.coa_segval5 AS coa_segval5,
    gbs.coa_segval6 AS coa_segval6,
    gbs.coa_segval7 AS coa_segval7,
    gbs.coa_segval8 AS coa_segval8,
    gbs.coa_segval9 AS coa_segval9,
    gbs.coa_segval10 AS coa_segval10,
    gbs.segment1 AS segment1,
    gbs.segment2 AS segment2,
    gbs.segment3 AS segment3,
    gbs.segment4 AS segment4,
    gbs.segment5 AS segment5,
    gbs.segment6 AS segment6,
    gbs.segment7 AS segment7,
    gbs.segment8 AS segment8,
    gbs.segment9 AS segment9,
    gbs.segment10 AS segment10,
    CASE
      WHEN gbs.actual_flag = 'A'
      THEN (
        (
          -COALESCE(gbs.period_net_dr, 0) + COALESCE(gbs.period_net_cr, 0)
        ) / 1000
      )
      ELSE 0
    END AS actual_amount_in_k,
    0 AS budget_amount_in_k,
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
    ) || '-' || COALESCE(gbs.ledger_id, 0) || '-' || COALESCE(gbs.currency_code, 'NA') || '-' || COALESCE(UPPER(gbs.period_name), 'NA') || '-' || COALESCE(gbs.actual_flag, 'X') || '-' || COALESCE(gbs.code_combination_id, 0) || '-' || COALESCE(gbs.translated_flag, 'X') || '-' || COALESCE(gbs.budget_version_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      gb.ledger_id,
      gb.currency_code,
      UPPER(gb.period_name) AS `period_name`,
      gb.period_year,
      gb.actual_flag,
      gb.encumbrance_type_id,
      gb.translated_flag,
      gb.budget_version_id,
      gcc.account_type,
      gcc.code_combination_id,
      gcc.chart_of_accounts_id,
      gb.period_num,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment1, '00') AS `coa_segval1`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment2, '00') AS `coa_segval2`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment3, '00') AS `coa_segval3`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment4, '00') AS `coa_segval4`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment5, '00') AS `coa_segval5`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment6, '00') AS `coa_segval6`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment7, '00') AS `coa_segval7`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment8, '00') AS `coa_segval8`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment9, '00') AS `coa_segval9`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment10, '00') AS `coa_segval10`,
      gcc.segment1,
      gcc.segment2,
      gcc.segment3,
      gcc.segment4,
      gcc.segment5,
      gcc.segment6,
      gcc.segment7,
      gcc.segment8,
      gcc.segment9,
      gcc.segment10,
      gb.period_net_cr,
      gb.period_net_dr,
      gb.quarter_to_date_cr,
      gb.quarter_to_date_dr,
      gb.project_to_date_cr,
      gb.project_to_date_dr,
      gb.begin_balance_cr,
      gb.begin_balance_dr
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_balances
      WHERE
        is_deleted_flg <> 'Y'
    ) AS gb
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_code_combinations
      WHERE
        is_deleted_flg <> 'Y'
    ) AS gcc
      ON gb.code_combination_id = gcc.code_combination_id
    WHERE
      1 = 1 AND gcc.summary_flag = 'N' AND gb.actual_flag = 'A'
  ) AS gbs
  WHERE
    1 = 1 AND gbs.account_type IN ('A', 'L', 'O')
  UNION ALL
  SELECT
    gbs.ledger_id AS ledger_id,
    gbs.chart_of_accounts_id AS chart_of_accounts_id,
    gbs.currency_code AS currency_code,
    gbs.code_combination_id AS code_combination_id,
    gbs.ledger_id || '-' || gbs.period_name AS period_name,
    gbs.period_name AS gl_period,
    gbs.period_year AS period_year,
    gbs.actual_flag,
    gbs.translated_flag,
    gbs.budget_version_id,
    gbs.account_type AS account_type,
    gbs.coa_segval3 AS coa_segval3,
    gbs.coa_segval2 AS coa_segval2,
    gbs.coa_segval1 AS coa_segval1,
    gbs.coa_segval4 AS coa_segval4,
    gbs.coa_segval5 AS coa_segval5,
    gbs.coa_segval6 AS coa_segval6,
    gbs.coa_segval7 AS coa_segval7,
    gbs.coa_segval8 AS coa_segval8,
    gbs.coa_segval9 AS coa_segval9,
    gbs.coa_segval10 AS coa_segval10,
    gbs.segment1 AS segment1,
    gbs.segment2 AS segment2,
    gbs.segment3 AS segment3,
    gbs.segment4 AS segment4,
    gbs.segment5 AS segment5,
    gbs.segment6 AS segment6,
    gbs.segment7 AS segment7,
    gbs.segment8 AS segment8,
    gbs.segment9 AS segment9,
    gbs.segment10 AS segment10,
    0 AS actual_amount_in_k,
    CASE
      WHEN gbs.actual_flag = 'B'
      THEN (
        (
          -COALESCE(gbs.period_net_dr, 0) - COALESCE(gbs.begin_balance_dr, 0) + COALESCE(gbs.begin_balance_cr, 0) + COALESCE(gbs.period_net_cr, 0)
        ) / 1000
      )
      ELSE 0
    END AS budget_amount_in_k,
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
    ) || '-' || COALESCE(gbs.ledger_id, 0) || '-' || COALESCE(gbs.currency_code, 'NA') || '-' || COALESCE(UPPER(gbs.period_name), 'NA') || '-' || COALESCE(gbs.actual_flag, 'X') || '-' || COALESCE(gbs.code_combination_id, 0) || '-' || COALESCE(gbs.translated_flag, 'X') || '-' || COALESCE(gbs.budget_version_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      gb.ledger_id,
      gb.currency_code,
      UPPER(gb.period_name) AS `period_name`,
      gb.period_year,
      gb.encumbrance_type_id,
      gb.actual_flag,
      gb.translated_flag,
      gb.budget_version_id,
      gcc.account_type,
      gcc.code_combination_id,
      gcc.chart_of_accounts_id,
      gb.period_num,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment1, '00') AS `coa_segval1`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment2, '00') AS `coa_segval2`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment3, '00') AS `coa_segval3`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment4, '00') AS `coa_segval4`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment5, '00') AS `coa_segval5`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment6, '00') AS `coa_segval6`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment7, '00') AS `coa_segval7`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment8, '00') AS `coa_segval8`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment9, '00') AS `coa_segval9`,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment10, '00') AS `coa_segval10`,
      gcc.segment1,
      gcc.segment2,
      gcc.segment3,
      gcc.segment4,
      gcc.segment5,
      gcc.segment6,
      gcc.segment7,
      gcc.segment8,
      gcc.segment9,
      gcc.segment10,
      gb.period_net_cr,
      gb.period_net_dr,
      gb.quarter_to_date_cr,
      gb.quarter_to_date_dr,
      gb.project_to_date_cr,
      gb.project_to_date_dr,
      gb.begin_balance_cr,
      gb.begin_balance_dr
    FROM (
      SELECT
        *
      FROM silver_bec_ods.gl_balances
      WHERE
        is_deleted_flg <> 'Y'
    ) AS gb
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_code_combinations
      WHERE
        is_deleted_flg <> 'Y'
    ) AS gcc
      ON gb.code_combination_id = gcc.code_combination_id
    WHERE
      1 = 1 AND gcc.summary_flag = 'N' AND gb.actual_flag = 'B'
  ) AS gbs
  WHERE
    1 = 1 AND gbs.account_type IN ('A', 'L', 'O')
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_bs_budget_activity' AND batch_name = 'gl';