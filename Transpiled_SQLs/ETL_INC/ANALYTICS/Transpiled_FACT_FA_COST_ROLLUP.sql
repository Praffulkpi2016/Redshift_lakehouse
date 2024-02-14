TRUNCATE table  table gold_bec_dwh.FACT_FA_COST_ROLLUP;
WITH accounts AS (
  SELECT DISTINCT
    fcb.asset_cost_acct AS gl_account,
    fcb.asset_cost_account_ccid AS ccid,
    'Asset Account' AS account_type,
    fcb.book_type_code,
    dga.segment1,
    dga.segment3,
    dga.segment3_desc,
    fbc.set_of_books_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.fa_category_books
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fcb, gold_bec_dwh.dim_gl_accounts AS dga, (
    SELECT
      *
    FROM silver_bec_ods.fa_book_controls
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fbc
  WHERE
    dga.code_combination_id = asset_cost_account_ccid
    AND fbc.book_type_code = fcb.book_type_code
  UNION
  SELECT DISTINCT
    fcb.deprn_reserve_acct AS gl_account,
    fcb.reserve_account_ccid AS ccid,
    'Deprn Reserve Account' AS account_type,
    fcb.book_type_code,
    dga.segment1,
    dga.segment3,
    dga.segment3_desc,
    fbc.set_of_books_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.fa_category_books
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fcb, gold_bec_dwh.dim_gl_accounts AS dga, (
    SELECT
      *
    FROM silver_bec_ods.fa_book_controls
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fbc
  WHERE
    dga.code_combination_id = reserve_account_ccid
    AND fbc.book_type_code = fcb.book_type_code
)
INSERT INTO gold_bec_dwh.FACT_FA_COST_ROLLUP
SELECT
  book_type_code,
  account_type,
  gl_account,
  segment3_desc,
  fiscal_year,
  period_name,
  period_num,
  begin_balance,
  end_balance,
  additions,
  adjustments,
  retirements,
  depreciations,
  retirement_reserve,
  transfer,
  reclass, /* audit columns */
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
  ) || '-' || COALESCE(book_type_code, 'NA') || '-' || COALESCE(gl_account, 'NA') || '-' || COALESCE(period_name, 'NA') AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    book_type_code,
    account_type,
    gl_account,
    segment3_desc,
    fiscal_year,
    period_name,
    period_num,
    SUM(begin_balance) AS begin_balance,
    SUM(end_balance) AS end_balance,
    SUM(additions) AS additions,
    SUM(adjustments) AS adjustments,
    SUM(retirement) AS retirements,
    SUM(depreciation) AS depreciations,
    SUM(retirement_reserve) AS retirement_reserve,
    SUM(transfer) AS transfer,
    SUM(reclass) AS reclass
  FROM (
    SELECT
      ffaa.book_type_code,
      acc.account_type,
      ffaa.gl_account,
      acc.segment3_desc,
      dap.fiscal_year,
      dap.period_name,
      dap.period_num,
      0 AS begin_balance,
      0 AS end_balance,
      SUM(ffaa.cost) AS additions,
      0 AS adjustments,
      0 AS retirement,
      0 AS depreciation,
      0 AS retirement_reserve,
      0 AS transfer,
      0 AS reclass
    FROM gold_bec_dwh.fact_fa_assets_addition AS ffaa, accounts AS acc, gold_bec_dwh.dim_asset_period AS dap
    WHERE
      ffaa.book_type_code = acc.book_type_code
      AND ffaa.gl_account = acc.gl_account
      AND dap.book_type_code = acc.book_type_code
      AND ffaa.date_effective BETWEEN dap.period_open_date AND dap.period_close_date
    GROUP BY
      ffaa.book_type_code,
      ffaa.gl_account,
      dap.fiscal_year,
      dap.period_name,
      dap.period_num,
      acc.account_type,
      acc.segment3_desc
    UNION
    SELECT
      ffca.book_type_code,
      acc.account_type,
      ffca.gl_account,
      acc.segment3_desc,
      dap.fiscal_year,
      dap.period_name,
      dap.period_num,
      0 AS begin_balance,
      0 AS end_balance,
      0 AS additions,
      SUM(ffca.change) AS adjustments,
      0 AS retirement,
      0 AS depreciation,
      0 AS retirement_reserve,
      0 AS transfer,
      0 AS reclass
    FROM gold_bec_dwh.fact_fa_cost_adj AS ffca, accounts AS acc, gold_bec_dwh.dim_asset_period AS dap
    WHERE
      ffca.book_type_code = acc.book_type_code
      AND ffca.gl_account = acc.gl_account
      AND dap.book_type_code = acc.book_type_code
      AND ffca.date_effective BETWEEN dap.period_open_date AND dap.period_close_date
    GROUP BY
      ffca.book_type_code,
      acc.account_type,
      ffca.gl_account,
      dap.fiscal_year,
      dap.period_name,
      dap.period_num,
      acc.segment3_desc
    UNION
    SELECT
      ffr.book_type_code,
      acc.account_type,
      ffr.account,
      acc.segment3_desc,
      dap.fiscal_year,
      dap.period_name,
      dap.period_num,
      0 AS begin_balance,
      0 AS end_balance,
      0 AS additions,
      0 AS adjustments,
      SUM(ffr.cost) AS retirement,
      0 AS depreciation,
      0 AS retirement_reserve,
      0 AS transfer,
      0 AS reclass
    FROM gold_bec_dwh.fact_fa_retirements AS ffr, accounts AS acc, gold_bec_dwh.dim_asset_period AS dap
    WHERE
      ffr.book_type_code = acc.book_type_code
      AND ffr.account = acc.gl_account
      AND dap.book_type_code = acc.book_type_code
      AND ffr.date_effective BETWEEN dap.period_open_date AND dap.period_close_date
    GROUP BY
      ffr.book_type_code,
      ffr.account,
      dap.fiscal_year,
      dap.period_name,
      dap.period_num,
      acc.account_type,
      acc.segment3_desc
    UNION
    SELECT
      ffr.book_type_code,
      acc.account_type,
      acc.segment3,
      acc.segment3_desc,
      dap.fiscal_year,
      dap.period_name,
      dap.period_num,
      0 AS begin_balance,
      0 AS end_balance,
      0 AS additions,
      0 AS adjustments,
      0 AS retirement,
      SUM(ffr.depreciation) AS depreciation,
      SUM(ffr.retirement) AS retirement_reserve,
      SUM(transfer) AS transfer,
      SUM(reclass) AS reclass
    FROM gold_bec_dwh.FACT_FA_ASSET_RESERVES AS ffr, accounts AS acc, gold_bec_dwh.dim_asset_period AS dap
    WHERE
      ffr.book_type_code = acc.book_type_code
      AND ffr.adjustment_ccid = acc.ccid
      AND dap.book_type_code = acc.book_type_code
      AND ffr.period_counter_created = dap.period_counter
    GROUP BY
      ffr.book_type_code,
      acc.segment3,
      dap.fiscal_year,
      dap.period_name,
      dap.period_num,
      acc.account_type,
      acc.segment3_desc
    UNION
    SELECT
      acc.book_type_code,
      acc.account_type,
      ftb.segment3,
      acc.segment3_desc,
      ftb.period_year,
      ftb.period_name,
      ftb.period_num,
      SUM(begin_balance),
      SUM(ending_balance),
      0 AS additions,
      0 AS adjustments,
      0 AS retirement,
      0 AS depreciation,
      0 AS retirement_reserve,
      0 AS transfer,
      0 AS reclass
    FROM gold_bec_dwh.fact_trial_balance AS ftb, accounts AS acc, gold_bec_dwh.dim_asset_period AS dap
    WHERE
      ftb.ledger_id = acc.set_of_books_id
      AND ftb.segment3 = acc.gl_account
      AND ftb.segment1 = acc.segment1
      AND dap.book_type_code = acc.book_type_code
      AND ftb.period_name = dap.period_name
    GROUP BY
      acc.book_type_code,
      ftb.segment3,
      ftb.period_year,
      ftb.period_name,
      ftb.period_num,
      acc.account_type,
      acc.segment3_desc
  )
  WHERE
    1 = 1
  GROUP BY
    book_type_code,
    account_type,
    gl_account,
    segment3_desc,
    fiscal_year,
    period_name,
    period_num
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_fa_cost_rollup' AND batch_name = 'fa';