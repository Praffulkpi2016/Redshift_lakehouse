DROP table IF EXISTS gold_bec_dwh.FACT_BS_BUDGET_COMPARE;
CREATE TABLE gold_bec_dwh.FACT_BS_BUDGET_COMPARE AS
(
  SELECT
    gbs.ledger_id,
    gbs.period_set_name,
    gbs.currency_code,
    gbs.period_name,
    gbs.gl_period,
    gbs.period_year,
    gbs.translated_flag,
    gbs.actual_flag,
    gbs.budget_name,
    gbs.budget_version_id,
    gbs.account_type,
    gbs.code_combination_id,
    gbs.chart_of_accounts_id,
    gbs.period_num,
    gbs.coa_segval1,
    gbs.coa_segval2,
    gbs.coa_segval3,
    gbs.coa_segval4,
    gbs.coa_segval5,
    gbs.coa_segval6,
    gbs.coa_segval7,
    gbs.coa_segval8,
    gbs.coa_segval9,
    gbs.coa_segval10,
    gbs.segment1,
    gbs.segment2,
    gbs.segment3,
    gbs.segment4,
    gbs.segment5,
    gbs.segment6,
    gbs.segment7,
    gbs.segment8,
    gbs.segment9,
    gbs.segment10,
    gbs.period_net_cr,
    gbs.period_net_dr,
    gbs.quarter_to_date_cr,
    gbs.quarter_to_date_dr,
    gbs.project_to_date_cr,
    gbs.project_to_date_dr,
    gbs.begin_balance_cr,
    gbs.begin_balance_dr,
    gbs.amount,
    gbs.amount_in_k,
    gbs.ending_balance,
    gbs.begin_balance,
    gbs.actual_amount_in_k,
    gbs.budget_amount_in_k,
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
    ) || '-' || COALESCE(gbs.ledger_id, 0) || '-' || COALESCE(gbs.period_set_name, 'NA') || '-' || COALESCE(gbs.currency_code, 'NA') || '-' || UPPER(COALESCE(gbs.period_name, 'NA')) || '-' || COALESCE(gbs.actual_flag, 'X') || '-' || COALESCE(gbs.code_combination_id, 0) || '-' || COALESCE(gbs.translated_flag, 'X') || '-' || COALESCE(gbs.budget_version_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      gb.ledger_id AS ledger_id,
      gl.period_set_name AS period_set_name,
      gb.currency_code AS currency_code,
      gb.period_name AS period_name,
      gb.period_name AS gl_period,
      gb.period_year AS period_year,
      gb.translated_flag,
      gb.actual_flag AS actual_flag,
      'ACTUAL' AS budget_name,
      999999 AS budget_version_id,
      gcc.account_type AS account_type,
      gcc.code_combination_id AS code_combination_id,
      gcc.chart_of_accounts_id AS chart_of_accounts_id,
      gb.period_num AS period_num,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment1, '00') AS coa_segval1,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment2, '00') AS coa_segval2,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment3, '00') AS coa_segval3,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment4, '00') AS coa_segval4,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment5, '00') AS coa_segval5,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment6, '00') AS coa_segval6,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment7, '00') AS coa_segval7,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment8, '00') AS coa_segval8,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment9, '00') AS coa_segval9,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment10, '00') AS coa_segval10,
      gcc.segment1 AS segment1,
      gcc.segment2 AS segment2,
      gcc.segment3 AS segment3,
      gcc.segment4 AS segment4,
      gcc.segment5 AS segment5,
      gcc.segment6 AS segment6,
      gcc.segment7 AS segment7,
      gcc.segment8 AS segment8,
      gcc.segment9 AS segment9,
      gcc.segment10 AS segment10,
      gb.period_net_cr AS period_net_cr,
      gb.period_net_dr AS period_net_dr,
      gb.quarter_to_date_cr AS quarter_to_date_cr,
      gb.quarter_to_date_dr AS quarter_to_date_dr,
      gb.project_to_date_cr AS project_to_date_cr,
      gb.project_to_date_dr AS project_to_date_dr,
      gb.begin_balance_cr AS begin_balance_cr,
      gb.begin_balance_dr AS begin_balance_dr,
      (
        -gb.period_net_dr + gb.period_net_cr
      ) * -1 AS amount,
      (
        (
          -gb.period_net_dr + gb.period_net_cr
        ) / 1000
      ) * -1 AS amount_in_k,
      (
        -gb.period_net_dr + gb.period_net_cr - gb.begin_balance_dr + gb.begin_balance_cr
      ) * -1 AS ending_balance,
      (
        gb.begin_balance_dr - gb.begin_balance_cr
      ) AS begin_balance,
      (
        (
          -gb.period_net_dr + gb.period_net_cr
        ) / 1000
      ) * -1 AS actual_amount_in_k,
      0 AS budget_amount_in_k
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
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_ledgers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS gl
      ON gb.ledger_id = gl.ledger_id
    WHERE
      1 = 1
      AND gb.actual_flag = 'A'
      AND gcc.account_type IN ('A', 'L', 'O')
      AND gcc.summary_flag = 'N'
    UNION ALL
    SELECT
      gb.ledger_id AS ledger_id,
      gl.period_set_name AS period_set_name,
      gb.currency_code AS currency_code,
      gb.period_name AS period_name,
      gb.period_name AS gl_period,
      gb.period_year AS period_year,
      gb.translated_flag,
      gb.actual_flag AS actual_flag,
      gbv.budget_name AS budget_name,
      gb.budget_version_id AS budget_version_id,
      gcc.account_type AS account_type,
      gcc.code_combination_id AS code_combination_id,
      gcc.chart_of_accounts_id AS chart_of_accounts_id,
      gb.period_num AS period_num,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment1, '00') AS coa_segval1,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment2, '00') AS coa_segval2,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment3, '00') AS coa_segval3,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment4, '00') AS coa_segval4,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment5, '00') AS coa_segval5,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment6, '00') AS coa_segval6,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment7, '00') AS coa_segval7,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment8, '00') AS coa_segval8,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment9, '00') AS coa_segval9,
      gcc.chart_of_accounts_id || '-' || COALESCE(gcc.segment10, '00') AS coa_segval10,
      gcc.segment1 AS segment1,
      gcc.segment2 AS segment2,
      gcc.segment3 AS segment3,
      gcc.segment4 AS segment4,
      gcc.segment5 AS segment5,
      gcc.segment6 AS segment6,
      gcc.segment7 AS segment7,
      gcc.segment8 AS segment8,
      gcc.segment9 AS segment9,
      gcc.segment10 AS segment10,
      gb.period_net_cr AS period_net_cr,
      gb.period_net_dr AS period_net_dr,
      gb.quarter_to_date_cr AS quarter_to_date_cr,
      gb.quarter_to_date_dr AS quarter_to_date_dr,
      gb.project_to_date_cr AS project_to_date_cr,
      gb.project_to_date_dr AS project_to_date_dr,
      gb.begin_balance_cr AS begin_balance_cr,
      gb.begin_balance_dr AS begin_balance_dr,
      (
        -gb.period_net_dr + gb.period_net_cr
      ) * -1 AS amount,
      (
        (
          -gb.period_net_dr + gb.period_net_cr
        ) / 1000
      ) * -1 AS amount_in_k,
      (
        -gb.period_net_dr + gb.period_net_cr - gb.begin_balance_dr + gb.begin_balance_cr
      ) * -1 AS ending_balance,
      (
        gb.begin_balance_dr - gb.begin_balance_cr
      ) AS begin_balance,
      0 AS actual_amount_in_k,
      (
        (
          -gb.period_net_dr + gb.period_net_cr
        ) / 1000
      ) * -1 AS budget_amount_in_k
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
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_budget_versions
      WHERE
        is_deleted_flg <> 'Y'
    ) AS gbv
      ON gbv.budget_version_id = gb.budget_version_id
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.gl_ledgers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS gl
      ON gb.ledger_id = gl.ledger_id
    WHERE
      1 = 1
      AND gb.actual_flag = 'B'
      AND gcc.account_type IN ('A', 'L', 'O')
      AND gcc.summary_flag = 'N'
  ) AS gbs
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_bs_budget_compare' AND batch_name = 'gl';