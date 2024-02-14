/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_REV_EXP_TREND
/* and fact_rev_exp_trend.translated_flag = gb.translated_flag */
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.gl_balances AS gb, silver_bec_ods.gl_code_combinations AS gcc, silver_bec_ods.gl_ledgers AS gl
    WHERE
      gb.code_combination_id = gcc.code_combination_id
      AND gb.ledger_id = gl.ledger_id
      AND (
        gb.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_rev_exp_trend' AND batch_name = 'gl'
        )
      )
      AND fact_rev_exp_trend.ledger_id = gb.ledger_id
      AND fact_rev_exp_trend.period_set_name = gl.period_set_name
      AND fact_rev_exp_trend.currency_code = gb.currency_code
      AND fact_rev_exp_trend.period_name = gb.period_name
      AND fact_rev_exp_trend.actual_flag = gb.actual_flag
      AND fact_rev_exp_trend.code_combination_id = gcc.code_combination_id
  );
/* Insert records */
INSERT INTO gold_bec_dwh.fact_rev_exp_trend
(
  SELECT
    ledger_id AS ledger_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ledger_id AS ledger_id_key,
    period_set_name AS period_set_name,
    chart_of_accounts_id AS chart_of_accounts_id,
    currency_code AS currency_code,
    period_name AS period_name,
    period_name AS gl_period,
    period_year AS period_year,
    code_combination_id AS code_combination_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || code_combination_id AS code_combination_id_key,
    account_type AS account_type,
    coa_segval3 AS coa_segval3,
    coa_segval2 AS coa_segval2,
    coa_segval1 AS coa_segval1,
    coa_segval4 AS coa_segval4,
    coa_segval5 AS coa_segval5,
    coa_segval6 AS coa_segval6,
    coa_segval7 AS coa_segval7,
    coa_segval8 AS coa_segval8,
    coa_segval9 AS coa_segval9,
    coa_segval10 AS coa_segval10,
    segment1 AS segment1,
    segment2 AS segment2,
    segment3 AS segment3,
    segment4 AS segment4,
    segment5 AS segment5,
    segment6 AS segment6,
    segment7 AS segment7,
    segment8 AS segment8,
    segment9 AS segment9,
    segment10 AS segment10,
    period_num,
    CASE WHEN period_num = 1 THEN (
      period_net_dr - period_net_cr
    ) ELSE 0 END AS period1_amount,
    CASE WHEN period_num = 2 THEN (
      period_net_dr - period_net_cr
    ) ELSE 0 END AS period2_amount,
    CASE WHEN period_num = 3 THEN (
      period_net_dr - period_net_cr
    ) ELSE 0 END AS period3_amount,
    CASE WHEN period_num = 4 THEN (
      period_net_dr - period_net_cr
    ) ELSE 0 END AS period4_amount,
    CASE WHEN period_num = 5 THEN (
      period_net_dr - period_net_cr
    ) ELSE 0 END AS period5_amount,
    CASE WHEN period_num = 6 THEN (
      period_net_dr - period_net_cr
    ) ELSE 0 END AS period6_amount,
    CASE WHEN period_num = 7 THEN (
      period_net_dr - period_net_cr
    ) ELSE 0 END AS period7_amount,
    CASE WHEN period_num = 8 THEN (
      period_net_dr - period_net_cr
    ) ELSE 0 END AS period8_amount,
    CASE WHEN period_num = 9 THEN (
      period_net_dr - period_net_cr
    ) ELSE 0 END AS period9_amount,
    CASE WHEN period_num = 10 THEN (
      period_net_dr - period_net_cr
    ) ELSE 0 END AS period10_amount,
    CASE WHEN period_num = 11 THEN (
      period_net_dr - period_net_cr
    ) ELSE 0 END AS period11_amount,
    CASE WHEN period_num = 12 THEN (
      period_net_dr - period_net_cr
    ) ELSE 0 END AS period12_amount,
    CASE
      WHEN period_num IN (1, 2, 3)
      THEN (
        period_net_dr - period_net_cr
      )
      ELSE 0
    END AS quarter1_amount,
    CASE
      WHEN period_num IN (4, 5, 6)
      THEN (
        period_net_dr - period_net_cr
      )
      ELSE 0
    END AS quarter2_amount,
    CASE
      WHEN period_num IN (7, 8, 9)
      THEN (
        period_net_dr - period_net_cr
      )
      ELSE 0
    END AS quarter3_amount,
    CASE
      WHEN period_num IN (10, 11, 12)
      THEN (
        period_net_dr - period_net_cr
      )
      ELSE 0
    END AS quarter4_amount,
    translated_flag,
    actual_flag,
    'N' AS IS_DELETED_FLG, /* audit columns */
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
    ) || '-' || COALESCE(ledger_id, 0) || '-' || COALESCE(period_set_name, 'NA') || '-' || COALESCE(currency_code, 'NA') || '-' || UPPER(COALESCE(period_name, 'NA')) || '-' || COALESCE(actual_flag, 'X') || '-' || COALESCE(code_combination_id, 0) || '-' || COALESCE(translated_flag, 'X') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      gb.ledger_id,
      gl.period_set_name,
      gb.currency_code,
      gb.period_name AS `period_name`,
      gb.period_year,
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
        is_deleted_flg <> 'Y' AND actual_flag = 'A'
    ) AS gb
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_code_combinations
      WHERE
        is_deleted_flg <> 'Y' AND summary_flag = 'N' AND account_type IN ('R', 'E')
    ) AS gcc
      ON gb.code_combination_id = gcc.code_combination_id
    INNER JOIN (
      SELECT
        period_set_name,
        ledger_id
      FROM silver_bec_ods.gl_ledgers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS gl
      ON gb.ledger_id = gl.ledger_id
    WHERE
      1 = 1
      AND (
        gb.kca_seq_date >= (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_rev_exp_trend' AND batch_name = 'gl'
        )
      )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_rev_exp_trend' AND batch_name = 'gl';