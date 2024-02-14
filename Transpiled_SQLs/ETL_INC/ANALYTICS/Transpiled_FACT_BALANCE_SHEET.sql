/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_BALANCE_SHEET
WHERE
  (COALESCE(ledger_id, 0), COALESCE(period_set_name, 'NA'), COALESCE(currency_code, 'NA'), COALESCE(period_name, 'NA'), COALESCE(actual_flag, 'X'), COALESCE(code_combination_id, 0), COALESCE(translated_flag, 'X'), COALESCE(budget_version_id, 0)) IN (
    SELECT
      COALESCE(ods.ledger_id, 0) AS ledger_id,
      COALESCE(ods.period_set_name, 'NA') AS period_set_name,
      COALESCE(ods.currency_code, 'NA') AS currency_code,
      COALESCE(ods.period_name, 'NA') AS period_name,
      COALESCE(ods.actual_flag, 'X') AS actual_flag,
      COALESCE(ods.code_combination_id, 0) AS code_combination_id,
      COALESCE(ods.translated_flag, 'X') AS translated_flag,
      COALESCE(ods.budget_version_id, 0) AS budget_version_id
    FROM gold_bec_dwh.FACT_BALANCE_SHEET AS dw, (
      SELECT
        gb.ledger_id AS ledger_id,
        gl.period_set_name AS period_set_name,
        gb.currency_code AS currency_code,
        UPPER(gb.period_name) AS period_name,
        gb.period_name AS gl_period,
        gb.period_year AS period_year,
        gb.actual_flag AS actual_flag,
        'ACTUAL' AS budget_name,
        gcc.account_type AS account_type,
        gcc.code_combination_id AS code_combination_id,
        gb.translated_flag,
        gb.budget_version_id
      FROM silver_bec_ods.gl_balances AS gb
      INNER JOIN silver_bec_ods.gl_code_combinations AS gcc
        ON gb.code_combination_id = gcc.code_combination_id
      INNER JOIN silver_bec_ods.gl_ledgers AS gl
        ON gb.ledger_id = gl.ledger_id
      WHERE
        gb.actual_flag = 'A'
        AND gcc.summary_flag = 'N'
        AND (
          gb.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_balance_sheet' AND batch_name = 'gl'
          )
          OR gl.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_balance_sheet' AND batch_name = 'gl'
          )
          OR gcc.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_balance_sheet' AND batch_name = 'gl'
          )
          OR gb.is_deleted_flg = 'Y'
          OR gcc.is_deleted_flg = 'Y'
          OR gl.is_deleted_flg = 'Y'
        )
      UNION ALL
      SELECT
        gb.ledger_id AS ledger_id,
        gl.period_set_name AS period_set_name,
        gb.currency_code AS currency_code,
        UPPER(gb.period_name) AS period_name,
        gb.period_name AS gl_period,
        gb.period_year AS period_year,
        gb.actual_flag AS actual_flag,
        gbv.budget_name AS budget_name,
        gcc.account_type AS account_type,
        gcc.code_combination_id AS code_combination_id,
        gb.translated_flag,
        gb.budget_version_id
      FROM silver_bec_ods.gl_balances AS gb
      INNER JOIN silver_bec_ods.gl_code_combinations AS gcc
        ON gb.code_combination_id = gcc.code_combination_id
      INNER JOIN silver_bec_ods.gl_budget_versions AS gbv
        ON gbv.budget_version_id = gb.budget_version_id
      INNER JOIN silver_bec_ods.gl_ledgers AS gl
        ON gb.ledger_id = gl.ledger_id
      WHERE
        gb.actual_flag = 'B'
        AND gcc.summary_flag = 'N'
        AND (
          gb.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_balance_sheet' AND batch_name = 'gl'
          )
          OR gl.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_balance_sheet' AND batch_name = 'gl'
          )
          OR gcc.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_balance_sheet' AND batch_name = 'gl'
          )
          OR gb.is_deleted_flg = 'Y'
          OR gcc.is_deleted_flg = 'Y'
          OR gbv.is_deleted_flg = 'Y'
          OR gl.is_deleted_flg = 'Y'
        ) /* and gcc.account_type in ('A', 'L', 'O') */
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.ledger_id, 0) || '-' || COALESCE(ods.period_set_name, 'NA') || '-' || COALESCE(ods.currency_code, 'NA') || '-' || UPPER(COALESCE(ods.period_name, 'NA')) || '-' || COALESCE(ods.actual_flag, 'X') || '-' || COALESCE(ods.code_combination_id, 0) || '-' || COALESCE(ods.translated_flag, 'X') || '-' || COALESCE(ods.budget_version_id, 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.FACT_BALANCE_SHEET (
  ledger_id,
  period_set_name,
  currency_code,
  period_name,
  gl_period,
  period_year,
  actual_flag,
  budget_name,
  account_type,
  code_combination_id,
  translated_flag,
  budget_version_id,
  chart_of_accounts_id,
  period_num,
  coa_segval1,
  coa_segval2,
  coa_segval3,
  coa_segval4,
  coa_segval5,
  coa_segval6,
  coa_segval7,
  coa_segval8,
  coa_segval9,
  coa_segval10,
  segment1,
  segment2,
  segment3,
  segment4,
  segment5,
  segment6,
  segment7,
  segment8,
  segment9,
  segment10,
  period_net_cr,
  period_net_dr,
  quarter_to_date_cr,
  quarter_to_date_dr,
  project_to_date_cr,
  project_to_date_dr,
  begin_balance_cr,
  begin_balance_dr,
  amount,
  amount_in_k,
  ending_balance,
  begin_balance,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    fbs.ledger_id,
    fbs.period_set_name,
    fbs.currency_code,
    fbs.period_name,
    fbs.gl_period,
    fbs.period_year,
    fbs.actual_flag,
    fbs.budget_name,
    fbs.account_type,
    fbs.code_combination_id,
    fbs.translated_flag,
    fbs.budget_version_id,
    fbs.chart_of_accounts_id,
    fbs.period_num,
    fbs.coa_segval1,
    fbs.coa_segval2,
    fbs.coa_segval3,
    fbs.coa_segval4,
    fbs.coa_segval5,
    fbs.coa_segval6,
    fbs.coa_segval7,
    fbs.coa_segval8,
    fbs.coa_segval9,
    fbs.coa_segval10,
    fbs.segment1,
    fbs.segment2,
    fbs.segment3,
    fbs.segment4,
    fbs.segment5,
    fbs.segment6,
    fbs.segment7,
    fbs.segment8,
    fbs.segment9,
    fbs.segment10,
    fbs.period_net_cr,
    fbs.period_net_dr,
    fbs.quarter_to_date_cr,
    fbs.quarter_to_date_dr,
    fbs.project_to_date_cr,
    fbs.project_to_date_dr,
    fbs.begin_balance_cr,
    fbs.begin_balance_dr,
    fbs.amount,
    fbs.amount_in_k,
    fbs.ending_balance,
    fbs.begin_balance,
    fbs.is_deleted_flg,
    fbs.source_app_id,
    fbs.dw_load_id,
    fbs.dw_insert_date,
    fbs.dw_update_date
  FROM (
    SELECT
      gb.ledger_id AS ledger_id,
      gl.period_set_name AS period_set_name,
      gb.currency_code AS currency_code,
      UPPER(gb.period_name) AS period_name,
      gb.period_name AS gl_period,
      gb.period_year AS period_year,
      gb.actual_flag AS actual_flag,
      'ACTUAL' AS budget_name,
      gcc.account_type AS account_type,
      gcc.code_combination_id AS code_combination_id,
      gb.translated_flag,
      gb.budget_version_id,
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
        gb.period_net_dr - gb.period_net_cr
      ) AS amount,
      (
        (
          gb.period_net_dr - gb.period_net_cr
        ) / 1000
      ) AS amount_in_k,
      (
        gb.begin_balance_dr - gb.begin_balance_cr
      ) /*	((gb.begin_balance_dr - gb.begin_balance_cr) - (gb.period_net_dr - gb.period_net_cr)) as ending_balance, */ - (
        gb.period_net_cr_beq - gb.period_net_dr_beq
      ) AS Ending_Balance,
      (
        gb.begin_balance_dr - gb.begin_balance_cr
      ) AS begin_balance,
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
      gb.actual_flag = 'A'
      AND gcc.summary_flag = 'N'
      AND (
        gb.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_balance_sheet' AND batch_name = 'gl'
        )
        OR gl.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_balance_sheet' AND batch_name = 'gl'
        )
        OR gcc.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_balance_sheet' AND batch_name = 'gl'
        )
      ) /* and gcc.account_type in ('A', 'L', 'O') */
    UNION ALL
    SELECT
      gb.ledger_id AS ledger_id,
      gl.period_set_name AS period_set_name,
      gb.currency_code AS currency_code,
      UPPER(gb.period_name) AS period_name,
      gb.period_name AS gl_period,
      gb.period_year AS period_year,
      gb.actual_flag AS actual_flag,
      gbv.budget_name AS budget_name,
      gcc.account_type AS account_type,
      gcc.code_combination_id AS code_combination_id,
      gb.translated_flag,
      gb.budget_version_id,
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
        gb.period_net_dr - gb.period_net_cr
      ) AS amount,
      (
        (
          gb.period_net_dr - gb.period_net_cr
        ) / 1000
      ) AS amount_in_k,
      (
        gb.begin_balance_dr - gb.begin_balance_cr
      ) /* ((gb.period_net_dr - gb.period_net_cr) - (gb.begin_balance_dr - gb.begin_balance_cr)) as ending_balance, */ - (
        gb.period_net_cr_beq - gb.period_net_dr_beq
      ) AS Ending_Balance,
      (
        gb.begin_balance_dr - gb.begin_balance_cr
      ) AS begin_balance,
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
      gb.actual_flag = 'B'
      AND gcc.summary_flag = 'N'
      AND (
        gb.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_balance_sheet' AND batch_name = 'gl'
        )
        OR gl.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_balance_sheet' AND batch_name = 'gl'
        )
        OR gcc.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_balance_sheet' AND batch_name = 'gl'
        )
      )
  ) AS fbs
);
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_balance_sheet' AND batch_name = 'gl';