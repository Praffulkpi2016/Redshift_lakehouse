DROP table IF EXISTS gold_bec_dwh.FACT_INCOME_STATEMENT;
CREATE TABLE gold_bec_dwh.fact_income_statement AS
(
  SELECT
    gb.ledger_id AS ledger_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || gb.ledger_id AS ledger_id_key,
    gl.period_set_name AS period_set_name,
    gb.currency_code AS currency_code,
    gb.period_name AS period_name,
    gb.period_name AS gl_period,
    gb.period_year AS period_year,
    gb.actual_flag AS actual_flag,
    gb.translated_flag,
    gb.budget_version_id,
    'ACTUAL' AS budget_name,
    gcc.account_type AS account_type,
    gcc.code_combination_id AS code_combination_id,
    gcc.chart_of_accounts_id AS chart_of_accounts_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || gcc.code_combination_id AS code_combination_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || gcc.chart_of_accounts_id AS chart_of_accounts_id_key,
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
      gb.period_net_dr - gb.period_net_cr
    ) AS amount,
    (
      (
        gb.period_net_dr - gb.period_net_cr
      ) / 1000
    ) AS amount_in_k,
    (
      gb.begin_balance_dr - gb.begin_balance_cr
    ) /* ((gb.begin_balance_dr - gb.begin_balance_cr) - (gb.period_net_dr - gb.period_net_cr)) as ending_balance, */ - (
      gb.period_net_cr_beq - gb.period_net_dr_beq
    ) AS ending_balance,
    (
      gb.begin_balance_dr - gb.begin_balance_cr
    ) AS begin_balance,
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
    ) || '-' || COALESCE(gb.ledger_id, 0) || '-' || COALESCE(gl.period_set_name, 'NA') || '-' || COALESCE(gb.currency_code, 'NA') || '-' || UPPER(COALESCE(gb.period_name, 'NA')) || '-' || COALESCE(gb.actual_flag, 'X') || '-' || COALESCE(gcc.code_combination_id, 0) || '-' || COALESCE(gb.translated_flag, 'X') || '-' || COALESCE(gb.budget_version_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
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
    1 = 1 AND gb.actual_flag = 'A' AND gcc.summary_flag = 'N' AND gcc.account_type IN ('R', 'E')
  UNION ALL
  SELECT
    gb.ledger_id AS ledger_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || gb.ledger_id AS ledger_id_key,
    gl.period_set_name AS period_set_name,
    gb.currency_code AS currency_code,
    gb.period_name AS period_name,
    gb.period_name AS gl_period,
    gb.period_year AS period_year,
    gb.actual_flag AS actual_flag,
    gb.translated_flag,
    gb.budget_version_id,
    gbv.budget_name AS budget_name,
    gcc.account_type AS account_type,
    gcc.code_combination_id AS code_combination_id,
    gcc.chart_of_accounts_id AS chart_of_accounts_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || gcc.code_combination_id AS code_combination_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || gcc.chart_of_accounts_id AS chart_of_accounts_id_key,
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
      gb.begin_balance_dr - gb.begin_balance_cr
    ) /* (-gb.period_net_dr + gb.period_net_cr-gb.begin_balance_dr+gb.begin_balance_cr)*-1 as ending_balance, */ - (
      gb.period_net_cr_beq - gb.period_net_dr_beq
    ) AS ending_balance,
    (
      gb.begin_balance_dr - gb.begin_balance_cr
    ) AS begin_balance,
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
    ) || '-' || COALESCE(gb.ledger_id, 0) || '-' || COALESCE(gl.period_set_name, 'NA') || '-' || COALESCE(gb.currency_code, 'NA') || '-' || UPPER(COALESCE(gb.period_name, 'NA')) || '-' || COALESCE(gb.actual_flag, 'X') || '-' || COALESCE(gcc.code_combination_id, 0) || '-' || COALESCE(gb.translated_flag, 'X') || '-' || COALESCE(gb.budget_version_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
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
    1 = 1 AND gb.actual_flag = 'B' AND gcc.summary_flag = 'N' AND gcc.account_type IN ('R', 'E')
);
UPDATE BEC_ETL_CTRL.BATCH_DW_INFO SET LOAD_TYPE = 'I', LAST_REFRESH_DATE = CURRENT_TIMESTAMP()
WHERE
  DW_TABLE_NAME = 'fact_income_statement' AND BATCH_NAME = 'gl';