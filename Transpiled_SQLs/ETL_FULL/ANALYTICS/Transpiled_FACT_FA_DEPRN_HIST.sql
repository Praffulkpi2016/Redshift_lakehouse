DROP table IF EXISTS gold_bec_dwh.FACT_FA_DEPRN_HIST;
CREATE TABLE gold_bec_dwh.FACT_FA_DEPRN_HIST AS
SELECT
  asset_id,
  book_type_code,
  dh_ccid,
  deprn_reserve_acct,
  rate,
  capacity,
  cost,
  deprn_reserve,
  ytd_deprn,
  deprn_amount,
  `percent`,
  transaction_type,
  period_counter,
  date_effective,
  bonus_deprn_expense_acct,
  duties_octroi,
  assigned_to,
  location_id,
  deprn_rate,
  category_id,
  gl_account,
  accu_deprn,
  distribution_id,
  addition_cost_to_clear,
  deprn_adjustment_amount,
  deprn_run_date,
  dh_distribution_id,
  deprn_source_code,
  event_id,
  period_name,
  system_deprn_amount,
  dps_deprn_amount,
  invoice_price,
  frieght_insurance,
  installation,
  other,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || ASSET_ID AS ASSET_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || LOCATION_ID AS LOCATION_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || DISTRIBUTION_ID AS DISTRIBUTION_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || category_id AS category_id_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || event_id AS event_id_KEY,
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
  ) || '-' || COALESCE(asset_id, 0) || '-' || COALESCE(distribution_id, 0) || '-' || COALESCE(book_type_code, 'NA') || '-' || COALESCE(period_name, 'NA') AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    asset_id,
    book_type_code,
    dh_ccid,
    deprn_reserve_acct,
    rate,
    capacity,
    cost,
    deprn_reserve,
    ytd_deprn,
    deprn_amount,
    `percent`,
    CASE
      WHEN transaction_type = 'P'
      THEN 'Partially Retired'
      WHEN transaction_type = 'F'
      THEN 'Fully Retired'
      WHEN transaction_type = 'T'
      THEN 'Transferred Out'
      WHEN transaction_type = 'N'
      THEN 'Non Depreciate'
      WHEN transaction_type = 'R'
      THEN 'Reclass'
      WHEN transaction_type = 'B'
      THEN 'Bonus'
      ELSE 'In Use'
    END AS transaction_type,
    period_counter,
    date_effective,
    bonus_deprn_expense_acct,
    CASE WHEN transaction_type = 'B' THEN NULL ELSE cost END AS duties_octroi,
    assigned_to,
    location_id,
    deprn_rate,
    category_id,
    gl_account,
    accu_deprn,
    distribution_id,
    addition_cost_to_clear,
    deprn_adjustment_amount,
    deprn_run_date,
    dh_distribution_id,
    deprn_source_code,
    event_id,
    period_name,
    system_deprn_amount,
    dps_deprn_amount,
    invoice_price,
    frieght_insurance,
    installation,
    other
  FROM (
    (
      WITH cte_asgn_loc AS (
        SELECT
          assigned_to,
          location_id,
          asset_id
        FROM silver_bec_ods.fa_distribution_history AS fdh
        WHERE
          distribution_id = (
            SELECT
              MAX(distribution_id)
            FROM silver_bec_ods.fa_distribution_history
            WHERE
              asset_id = fdh.asset_id
          )
      )
      SELECT
        dh.asset_id AS asset_id,
        cb.book_type_code,
        dh.code_combination_id AS dh_ccid,
        cb.deprn_reserve_acct AS deprn_reserve_acct,
        books.adjusted_rate AS rate,
        books.production_capacity AS capacity,
        dd_bonus.cost AS cost,
        CASE
          WHEN dd_bonus.period_counter = dp.upc
          OR (
            dd_bonus.period_counter IS NULL AND dp.upc IS NULL
          )
          THEN dd_bonus.deprn_amount - dd_bonus.bonus_deprn_amount
          ELSE 0
        END AS deprn_amount,
        CASE
          WHEN SIGN(dp.tpc - dd_bonus.period_counter) = 1
          THEN 0
          ELSE dd_bonus.ytd_deprn - dd_bonus.bonus_ytd_deprn
        END AS ytd_deprn,
        dd_bonus.deprn_reserve - dd_bonus.bonus_deprn_reserve AS deprn_reserve,
        CASE
          WHEN th.transaction_type_code IS NULL
          THEN dh.units_assigned / ah.units * 100
        END AS `percent`,
        CASE
          WHEN th.transaction_type_code IS NULL
          THEN CASE
            WHEN th_rt.transaction_type_code = 'FULL RETIREMENT'
            THEN 'F'
            ELSE CASE WHEN books.depreciate_flag = 'NO' THEN 'N' END
          END
          WHEN th.transaction_type_code = 'TRANSFER'
          THEN 'T'
          WHEN th.transaction_type_code = 'TRANSFER OUT'
          THEN 'P'
          WHEN th.transaction_type_code = 'RECLASS'
          THEN 'R'
        END AS transaction_type,
        dp.upc AS period_counter,
        COALESCE(th.date_effective, dp.ucd) AS date_effective,
        '' AS bonus_deprn_expense_acct,
        cte_asgn_loc.assigned_to,
        cte_asgn_loc.location_id,
        books.basic_rate * 100 AS deprn_rate,
        ah.category_id,
        cb.asset_cost_acct AS gl_account,
        cb.deprn_reserve_acct AS accu_deprn,
        dd_bonus.distribution_id,
        dd_bonus.addition_cost_to_clear,
        dd_bonus.deprn_adjustment_amount,
        dd_bonus.deprn_run_date,
        dh.distribution_id AS dh_distribution_id,
        dd_bonus.deprn_source_code,
        dd_bonus.event_id,
        dp.period_name,
        ds.system_deprn_amount,
        ds.deprn_amount AS dps_deprn_amount,
        inv.invoice_price,
        inv.frieght_insurance,
        inv.installation,
        inv.other
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fa_deprn_detail
        WHERE
          is_deleted_flg <> 'Y'
      ) AS dd_bonus
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_category_books
        WHERE
          is_deleted_flg <> 'Y'
      ) AS cb
        ON dd_bonus.book_type_code = cb.book_type_code
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_distribution_history
        WHERE
          is_deleted_flg <> 'Y'
      ) AS dh
        ON dd_bonus.distribution_id = dh.distribution_id
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_books
        WHERE
          is_deleted_flg <> 'Y'
      ) AS books
        ON cb.book_type_code = books.book_type_code AND dh.asset_id = books.asset_id
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_asset_history
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ah
        ON cb.category_id = ah.category_id AND dh.asset_id = ah.asset_id
      INNER JOIN (
        SELECT
          bc.distribution_source_book AS dist_book,
          COALESCE(dp.period_close_date, CURRENT_TIMESTAMP()) AS ucd,
          dp.period_counter AS upc,
          MIN(dp_fy.period_open_date) AS tod,
          MIN(dp_fy.period_counter) AS tpc,
          dp.period_name,
          bc.book_type_code
        FROM (
          SELECT
            *
          FROM silver_bec_ods.fa_deprn_periods
          WHERE
            is_deleted_flg <> 'Y'
        ) AS dp, (
          SELECT
            *
          FROM silver_bec_ods.fa_deprn_periods
          WHERE
            is_deleted_flg <> 'Y'
        ) AS dp_fy, (
          SELECT
            *
          FROM silver_bec_ods.fa_book_controls
          WHERE
            is_deleted_flg <> 'Y'
        ) AS bc
        WHERE
          dp.book_type_code = bc.book_type_code
          AND dp_fy.book_type_code = bc.book_type_code
          AND dp_fy.fiscal_year = dp.fiscal_year
        GROUP BY
          bc.distribution_source_book,
          dp.period_close_date,
          dp.period_counter,
          dp.period_name,
          bc.book_type_code
      ) AS dp
        ON dh.book_type_code = dp.dist_book
        AND dh.date_effective <= dp.ucd
        AND COALESCE(dh.date_ineffective, CURRENT_TIMESTAMP()) > dp.tod
        AND books.book_type_code = dp.book_type_code
        AND COALESCE(books.period_counter_fully_retired, dp.upc) >= dp.tpc
        AND dd_bonus.period_counter = (
          SELECT
            MAX(dd_sub.period_counter)
          FROM (
            SELECT
              *
            FROM silver_bec_ods.fa_deprn_detail
            WHERE
              is_deleted_flg <> 'Y'
          ) AS dd_sub
          WHERE
            dd_sub.book_type_code = cb.book_type_code
            AND dd_sub.asset_id = dh.asset_id
            AND dd_sub.distribution_id = dh.distribution_id
            AND dd_sub.period_counter <= dp.upc
        )
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_transaction_headers
        WHERE
          is_deleted_flg <> 'Y'
      ) AS th_rt
        ON cb.book_type_code = th_rt.book_type_code
        AND books.transaction_header_id_in = th_rt.transaction_header_id
      LEFT OUTER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_transaction_headers
        WHERE
          is_deleted_flg <> 'Y'
      ) AS th
        ON dp.dist_book = th.book_type_code
        AND dh.transaction_header_id_out = th.transaction_header_id
        AND th.date_effective BETWEEN dp.tod AND dp.ucd
      LEFT OUTER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_deprn_summary
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ds
        ON dp.upc = ds.period_counter
        AND books.book_type_code = ds.book_type_code
        AND books.asset_id = ds.asset_id
      LEFT OUTER JOIN (
        SELECT
          SUM(attribute3) AS invoice_price,
          SUM(attribute4) AS frieght_insurance,
          SUM(attribute6) AS installation,
          SUM(attribute7) AS other,
          asset_id
        FROM silver_bec_ods.fa_asset_invoices
        GROUP BY
          asset_id
      ) AS inv
        ON inv.asset_id = dh.asset_id
      LEFT OUTER JOIN cte_asgn_loc
        ON cte_asgn_loc.asset_id = dh.asset_id
      WHERE
        1 = 1
        AND ah.asset_type = 'CAPITALIZED'
        AND ah.date_effective < COALESCE(th.date_effective, dp.ucd)
        AND COALESCE(ah.date_ineffective, CURRENT_TIMESTAMP()) >= COALESCE(th.date_effective, dp.ucd)
        AND books.date_effective <= COALESCE(th.date_effective, dp.ucd)
        AND COALESCE(books.date_ineffective, CURRENT_TIMESTAMP() + 1) > COALESCE(th.date_effective, dp.ucd)
    )
    UNION ALL
    (
      WITH cte_asgn_loc AS (
        SELECT
          assigned_to,
          location_id,
          asset_id
        FROM silver_bec_ods.fa_distribution_history AS fdh
        WHERE
          distribution_id = (
            SELECT
              MAX(distribution_id)
            FROM silver_bec_ods.fa_distribution_history
          ) /* where asset_id = dh.asset_id */
      )
      SELECT
        dh.asset_id AS asset_id,
        cb.book_type_code,
        dh.code_combination_id AS dh_ccid,
        cb.bonus_deprn_reserve_acct AS deprn_reserve_acct,
        books.adjusted_rate AS rate,
        books.production_capacity AS capacity,
        0 AS cost,
        CASE
          WHEN dd.period_counter = dp.upc OR (
            dd.period_counter IS NULL AND dp.upc IS NULL
          )
          THEN dd.bonus_deprn_amount
          ELSE 0
        END AS deprn_amount,
        CASE WHEN SIGN(dp.tpc - dd.period_counter) = 1 THEN 0 ELSE dd.bonus_ytd_deprn END AS ytd_deprn,
        dd.bonus_deprn_reserve AS deprn_reserve,
        0 AS `percent`,
        'B' AS transaction_type,
        dp.upc AS period_counter,
        COALESCE(th.date_effective, dp.ucd) AS date_effective,
        cb.bonus_deprn_expense_acct AS bonus_deprn_expense_acct,
        cte_asgn_loc.assigned_to,
        cte_asgn_loc.location_id,
        books.basic_rate * 100 AS deprn_rate,
        ah.category_id,
        cb.asset_cost_acct AS gl_account,
        cb.deprn_reserve_acct AS accu_deprn,
        dd.distribution_id,
        dd.addition_cost_to_clear,
        dd.deprn_adjustment_amount,
        dd.deprn_run_date,
        dh.distribution_id AS dh_distribution_id,
        dd.deprn_source_code,
        dd.event_id,
        dp.period_name,
        ds.system_deprn_amount,
        ds.deprn_amount AS dps_deprn_amount,
        inv.invoice_price,
        inv.frieght_insurance,
        inv.installation,
        inv.other
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fa_deprn_detail
        WHERE
          is_deleted_flg <> 'Y'
      ) AS dd
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_category_books
        WHERE
          is_deleted_flg <> 'Y'
      ) AS cb
        ON dd.book_type_code = cb.book_type_code
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_distribution_history
        WHERE
          is_deleted_flg <> 'Y'
      ) AS dh
        ON dd.distribution_id = dh.distribution_id
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_books
        WHERE
          is_deleted_flg <> 'Y'
      ) AS books
        ON cb.book_type_code = books.book_type_code AND dh.asset_id = books.asset_id
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_asset_history
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ah
        ON cb.category_id = ah.category_id AND dh.asset_id = ah.asset_id
      INNER JOIN (
        SELECT
          bc.distribution_source_book AS dist_book,
          COALESCE(dp.period_close_date, CURRENT_TIMESTAMP()) AS ucd,
          dp.period_counter AS upc,
          MIN(dp_fy.period_open_date) AS tod,
          MIN(dp_fy.period_counter) AS tpc,
          dp.period_name,
          bc.book_type_code
        FROM (
          SELECT
            *
          FROM silver_bec_ods.fa_deprn_periods
          WHERE
            is_deleted_flg <> 'Y'
        ) AS dp, (
          SELECT
            *
          FROM silver_bec_ods.fa_deprn_periods
          WHERE
            is_deleted_flg <> 'Y'
        ) AS dp_fy, (
          SELECT
            *
          FROM silver_bec_ods.fa_book_controls
          WHERE
            is_deleted_flg <> 'Y'
        ) AS bc
        WHERE
          dp.book_type_code = bc.book_type_code
          AND dp_fy.book_type_code = bc.book_type_code
          AND dp_fy.fiscal_year = dp.fiscal_year
        GROUP BY
          bc.distribution_source_book,
          dp.period_close_date,
          dp.period_counter,
          dp.period_name,
          bc.book_type_code
      ) AS dp
        ON dh.book_type_code = dp.dist_book
        AND dh.date_effective <= dp.ucd
        AND COALESCE(dh.date_ineffective, CURRENT_TIMESTAMP()) > dp.tod
        AND books.book_type_code = dp.book_type_code
        AND COALESCE(books.period_counter_fully_retired, dp.upc) >= dp.tpc
        AND dd.period_counter = (
          SELECT
            MAX(dd_sub.period_counter)
          FROM (
            SELECT
              *
            FROM silver_bec_ods.fa_deprn_detail
            WHERE
              is_deleted_flg <> 'Y'
          ) AS dd_sub
          WHERE
            dd_sub.book_type_code = cb.book_type_code
            AND dd_sub.asset_id = dh.asset_id
            AND dd_sub.distribution_id = dh.distribution_id
            AND dd_sub.period_counter <= dp.upc
        )
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_transaction_headers
        WHERE
          is_deleted_flg <> 'Y'
      ) AS th_rt
        ON cb.book_type_code = th_rt.book_type_code
        AND books.transaction_header_id_in = th_rt.transaction_header_id
      LEFT OUTER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_transaction_headers
        WHERE
          is_deleted_flg <> 'Y'
      ) AS th
        ON dp.dist_book = th.book_type_code
        AND dh.transaction_header_id_out = th.transaction_header_id
        AND th.date_effective BETWEEN dp.tod AND dp.ucd
      LEFT OUTER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_deprn_summary
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ds
        ON dp.upc = ds.period_counter
        AND books.book_type_code = ds.book_type_code
        AND books.asset_id = ds.asset_id
      LEFT OUTER JOIN (
        SELECT
          SUM(attribute3) AS invoice_price,
          SUM(attribute4) AS frieght_insurance,
          SUM(attribute6) AS installation,
          SUM(attribute7) AS other,
          asset_id
        FROM silver_bec_ods.fa_asset_invoices
        GROUP BY
          asset_id
      ) AS inv
        ON inv.asset_id = dh.asset_id
      LEFT OUTER JOIN cte_asgn_loc
        ON cte_asgn_loc.asset_id = dh.asset_id
      WHERE
        1 = 1
        AND ah.asset_type = 'CAPITALIZED'
        AND ah.date_effective < COALESCE(th.date_effective, dp.ucd)
        AND COALESCE(ah.date_ineffective, CURRENT_TIMESTAMP()) >= COALESCE(th.date_effective, dp.ucd)
        AND books.date_effective <= COALESCE(th.date_effective, dp.ucd)
        AND COALESCE(books.date_ineffective, CURRENT_TIMESTAMP() + 1) > COALESCE(th.date_effective, dp.ucd)
        AND NOT books.bonus_rule IS NULL
    )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_fa_deprn_hist' AND batch_name = 'fa';