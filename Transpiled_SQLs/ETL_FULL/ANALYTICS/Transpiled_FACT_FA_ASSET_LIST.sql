DROP table IF EXISTS gold_bec_dwh.FACT_FA_ASSET_LIST;
CREATE TABLE gold_bec_dwh.FACT_FA_ASSET_LIST AS
SELECT
  A.PERIOD_NAME,
  A.PRORATE_DATE,
  A.SET_OF_BOOKS_ID,
  A.ORIGINAL_COST,
  A.SALVAGE_VALUE,
  A.BOOK_TYPE_CODE,
  A.ASSET_ID,
  A.DATE_PLACED_IN_SERVICE,
  A.DEPRN_METHOD_CODE,
  A.LIFE_MONTHS,
  A.LIFE_YEARS,
  A.REMAINDER_LIFE_MONTHS,
  A.REMAINDER_LIFE_YEARS,
  A.FB_PRORATE_DATE,
  A.COST,
  A.DEPRECIATE_FLAG,
  A.RETIREMENT_ID,
  A.PERIOD_COUNTER,
  A.DEPRN_SOURCE_CODE,
  A.DEPRN_RUN_DATE,
  A.DEPRN_AMOUNT,
  A.YTD_DEPRN,
  A.DEPRN_RESERVE,
  A.ADDITION_COST_TO_CLEAR,
  A.NET_BOOK_VALUE,
  A.DIST_COST,
  A.DEPRN_ADJUSTMENT_AMOUNT,
  A.BONUS_DEPRN_ADJUSTMENT_AMOUNT,
  A.CATEGORY_ID,
  A.UNITS_ASSIGNED,
  A.LOCATION_ID,
  A.ASSIGNED_TO,
  A.DISTRIBUTION_ID,
  A.DEPRN_AMOUNT_SUM,
  A.YTD_DERPN_SUM,
  A.DEPRN_RESERVE_SUM,
  A.NET_BOOK_VALUE_SUM,
  A.DATE_EFFECTIVE,
  A.CODE_COMBINATION_ID,
  A.DATE_EFFECTIVE_FB,
  A.DATE_EFFECTIVE_FDH,
  A.TRANSACTION_HEADER_ID_IN,
  A.ASSET_TYPE,
  'N' AS is_deleted_flg,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || A.SET_OF_BOOKS_ID AS SET_OF_BOOKS_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || A.ASSET_ID AS ASSET_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || A.RETIREMENT_ID AS RETIREMENT_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || A.CATEGORY_ID AS CATEGORY_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || A.LOCATION_ID AS LOCATION_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || A.DISTRIBUTION_ID AS DISTRIBUTION_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || A.CODE_COMBINATION_ID AS CODE_COMBINATION_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || A.TRANSACTION_HEADER_ID_IN AS TRANSACTION_HEADER_ID_IN_KEY,
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
  ) || '-' || COALESCE(A.asset_id, 0) || '-' || COALESCE(A.period_name, 'NA') || '-' || COALESCE(A.set_of_books_id, 0) || '-' || COALESCE(A.period_counter, 0) || '-' || COALESCE(A.transaction_header_id_in, 0) || '-' || COALESCE(A.distribution_id, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    fdp.PERIOD_NAME,
    PRORATE_DATE,
    fbc.set_of_books_id,
    fb.ORIGINAL_COST,
    fb.SALVAGE_VALUE,
    fb.BOOK_TYPE_CODE,
    fb.ASSET_ID,
    fb.DATE_PLACED_IN_SERVICE,
    fb.DEPRN_METHOD_CODE,
    fb.LIFE_IN_MONTHS AS LIFE_MONTHS,
    fb.LIFE_IN_MONTHS / CAST(12 AS DECIMAL(20, 2)) AS LIFE_YEARS,
    fb.LIFE_IN_MONTHS - CAST(MONTHS_BETWEEN(TRUNC(TO_DATE(fdp.PERIOD_NAME, 'MON YY')), TRUNC(fb.PRORATE_DATE)) AS INT) AS REMAINDER_LIFE_MONTHS,
    (
      fb.LIFE_IN_MONTHS - CAST(MONTHS_BETWEEN(FLOOR(TO_DATE(fdp.PERIOD_NAME, 'MON YY')), TRUNC(fb.PRORATE_DATE)) AS INT)
    ) / CAST(12 AS DECIMAL(20, 2)) AS REMAINDER_LIFE_YEARS,
    fb.PRORATE_DATE AS fb_PRORATE_DATE,
    fb.cost,
    fb.DEPRECIATE_FLAG,
    fb.RETIREMENT_ID,
    fdp.PERIOD_COUNTER,
    fdd.DEPRN_SOURCE_CODE,
    fdd.DEPRN_RUN_DATE,
    CASE
      WHEN fdp.period_counter - fdd.period_counter = 0
      THEN fdd.DEPRN_AMOUNT
      ELSE 0
    END AS DEPRN_AMOUNT,
    CASE
      WHEN SUBSTRING(fdp.period_name, 5, 2) = SUBSTRING(fdp3.period_name, 5, 2)
      OR (
        SUBSTRING(fdp.period_name, 5, 2) IS NULL
        AND SUBSTRING(fdp3.period_name, 5, 2) IS NULL
      )
      THEN fdd.YTD_DEPRN
      ELSE 0
    END AS ytd_deprn,
    fdd.DEPRN_RESERVE,
    fdd.ADDITION_COST_TO_CLEAR,
    CASE
      WHEN fdd.DEPRN_SOURCE_CODE = 'B'
      THEN fdd.ADDITION_COST_TO_CLEAR
      ELSE fdd.COST
    END - COALESCE(fdd.DEPRN_RESERVE, 0) AS Net_Book_Value,
    CASE
      WHEN fdd.DEPRN_SOURCE_CODE = 'B'
      THEN fdd.ADDITION_COST_TO_CLEAR
      ELSE fdd.COST
    END AS dist_Cost,
    fdd.DEPRN_ADJUSTMENT_AMOUNT,
    fdd.BONUS_DEPRN_ADJUSTMENT_AMOUNT,
    fah.category_id,
    fdh.units_assigned,
    fdh.LOCATION_ID,
    COALESCE(fdh.ASSIGNED_TO, -23453) AS ASSIGNED_TO,
    fdh.DISTRIBUTION_ID,
    CASE
      WHEN fdp.period_counter - fds.period_counter = 0
      THEN fds.DEPRN_AMOUNT
      ELSE 0
    END AS DEPRN_AMOUNT_SUM,
    CASE
      WHEN SUBSTRING(fdp.period_name, 5, 2) = SUBSTRING(fdp2.period_name, 5, 2)
      OR (
        SUBSTRING(fdp.period_name, 5, 2) IS NULL
        AND SUBSTRING(fdp2.period_name, 5, 2) IS NULL
      )
      THEN fds.YTD_DEPRN
      ELSE 0
    END AS YTD_DERPN_SUM,
    FDS.DEPRN_RESERVE AS DEPRN_RESERVE_SUM,
    FB.COST - COALESCE(FDS.DEPRN_RESERVE, 0) AS Net_Book_Value_SUM,
    fah.DATE_EFFECTIVE,
    fdh.CODE_COMBINATION_ID,
    fb.date_effective AS date_effective_fb,
    fdh.date_effective AS date_effective_fdh,
    fb.TRANSACTION_HEADER_ID_IN,
    fab.asset_type
  FROM (
    SELECT
      *
    FROM silver_bec_ods.fa_books
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fb
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_book_controls
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fbc
    ON fb.book_type_code = fbc.book_type_code
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_deprn_detail
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fdd
    ON fb.book_type_code = fdd.book_type_code AND fb.asset_id = fdd.asset_id
  INNER JOIN (
    SELECT
      PERIOD_NAME,
      PERIOD_COUNTER,
      period_close_date,
      book_type_code,
      period_open_date
    FROM (
      SELECT
        *
      FROM silver_bec_ods.fa_deprn_periods
      WHERE
        is_deleted_flg <> 'Y'
    )
  ) AS fdp
    ON fb.book_type_code = fdp.book_type_code
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_additions_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fab
    ON fb.asset_id = fab.asset_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_asset_history
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fah
    ON fab.asset_id = fah.asset_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_distribution_history
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fdh
    ON fdh.asset_id = fb.asset_id AND fdd.distribution_id = fdh.distribution_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_deprn_summary
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fds
    ON fb.book_type_code = fds.book_type_code
    AND fb.asset_id = fds.asset_id
    AND fdd.period_counter = fds.period_counter
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_deprn_periods
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fdp2
    ON fdp2.period_counter = fds.period_counter AND fdp2.book_type_code = fds.book_type_code
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_deprn_periods
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fdp3
    ON fdp3.period_counter = fdd.period_counter AND fdp3.book_type_code = fdd.book_type_code
  WHERE
    fb.date_effective <= COALESCE(fdp.period_close_date, CURRENT_TIMESTAMP())
    AND COALESCE(fb.date_ineffective, CURRENT_TIMESTAMP()) >= COALESCE(fdp.period_close_date, CURRENT_TIMESTAMP())
    AND (
      fdh.date_effective <= COALESCE(fdp.period_close_date, CURRENT_TIMESTAMP())
      AND COALESCE(fdh.date_ineffective, CURRENT_TIMESTAMP()) >= COALESCE(fdp.period_close_date, CURRENT_TIMESTAMP())
      OR COALESCE(fdh.date_ineffective, CURRENT_TIMESTAMP()) >= fdp.period_open_date
      AND COALESCE(fdh.date_ineffective, CURRENT_TIMESTAMP()) <= fdp.period_close_date
    )
    AND fdd.period_counter = (
      SELECT
        MAX(period_counter)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fa_deprn_detail
        WHERE
          is_deleted_flg <> 'Y'
      ) AS dd
      WHERE
        dd.book_type_code = fdp.book_type_code
        AND dd.asset_id = fb.asset_id
        AND dd.distribution_id = fdd.distribution_id
        AND dd.period_counter <= fdp.period_counter
    )
    AND fah.date_effective <= COALESCE(fdp.period_close_date, CURRENT_TIMESTAMP())
    AND COALESCE(fah.date_ineffective, CURRENT_TIMESTAMP()) >= COALESCE(fdp.period_close_date, CURRENT_TIMESTAMP())
    AND COALESCE(fb.period_counter_fully_retired, fdp.period_counter + 1) >= fdp.period_counter
  UNION ALL
  SELECT
    fdp.PERIOD_NAME,
    PRORATE_DATE,
    fbc.set_of_books_id,
    fb.ORIGINAL_COST,
    fb.SALVAGE_VALUE,
    fb.BOOK_TYPE_CODE,
    fb.ASSET_ID,
    fb.DATE_PLACED_IN_SERVICE,
    fb.DEPRN_METHOD_CODE,
    fb.LIFE_IN_MONTHS AS LIFE_MONTHS,
    fb.LIFE_IN_MONTHS / CAST(12 AS DECIMAL(20, 2)) AS LIFE_YEARS,
    fb.LIFE_IN_MONTHS - CAST(MONTHS_BETWEEN(TRUNC(TO_DATE(fdp.PERIOD_NAME, 'MON YY')), TRUNC(fb.PRORATE_DATE)) AS INT) AS REMAINDER_LIFE_MONTHS,
    (
      fb.LIFE_IN_MONTHS - CAST(MONTHS_BETWEEN(TRUNC(TO_DATE(fdp.PERIOD_NAME, 'MON YY')), TRUNC(fb.PRORATE_DATE)) AS INT)
    ) / CAST(12 AS DECIMAL(20, 2)) AS REMAINDER_LIFE_YEARS,
    fb.PRORATE_DATE,
    fb.cost,
    fb.DEPRECIATE_FLAG,
    fb.RETIREMENT_ID,
    fdp.PERIOD_COUNTER,
    fdd.DEPRN_SOURCE_CODE,
    fdd.DEPRN_RUN_DATE,
    CASE
      WHEN fdp.period_counter - fdd.period_counter = 0
      THEN fdd.DEPRN_AMOUNT
      ELSE 0
    END AS DEPRN_AMOUNT,
    CASE
      WHEN SUBSTRING(fdp.period_name, 5, 2) = SUBSTRING(fdp3.period_name, 5, 2)
      OR (
        SUBSTRING(fdp.period_name, 5, 2) IS NULL
        AND SUBSTRING(fdp3.period_name, 5, 2) IS NULL
      )
      THEN fdd.YTD_DEPRN
      ELSE 0
    END AS ytd_deprn,
    fdd.DEPRN_RESERVE,
    fdd.ADDITION_COST_TO_CLEAR,
    CASE
      WHEN fdd.DEPRN_SOURCE_CODE = 'B'
      THEN fdd.ADDITION_COST_TO_CLEAR
      ELSE fdd.COST
    END - COALESCE(fdd.DEPRN_RESERVE, 0) AS Net_Book_Value,
    CASE
      WHEN fdd.DEPRN_SOURCE_CODE = 'B'
      THEN fdd.ADDITION_COST_TO_CLEAR
      ELSE fdd.COST
    END AS dist_Cost,
    fdd.DEPRN_ADJUSTMENT_AMOUNT,
    fdd.BONUS_DEPRN_ADJUSTMENT_AMOUNT,
    fah.category_id,
    fdh.units_assigned,
    fdh.LOCATION_ID,
    COALESCE(fdh.ASSIGNED_TO, -23453) AS ASSIGNED_TO,
    fdh.DISTRIBUTION_ID,
    CASE
      WHEN fdp.period_counter - fds.period_counter = 0
      THEN fds.DEPRN_AMOUNT
      ELSE 0
    END AS DEPRN_AMOUNT_SUM,
    CASE
      WHEN SUBSTRING(fdp.period_name, 5, 2) = SUBSTRING(fdp2.period_name, 5, 2)
      OR (
        SUBSTRING(fdp.period_name, 5, 2) IS NULL
        AND SUBSTRING(fdp2.period_name, 5, 2) IS NULL
      )
      THEN fds.YTD_DEPRN
      ELSE 0
    END AS YTD_DERPN_SUM,
    FDS.DEPRN_RESERVE AS DEPRN_RESERVE_SUM,
    FB.COST - COALESCE(FDS.DEPRN_RESERVE, 0) AS Net_Book_Value_SUM,
    fah.DATE_EFFECTIVE,
    fdh.CODE_COMBINATION_ID,
    fb.date_effective AS date_effective_fb,
    fdh.date_effective AS date_effective_fdh,
    fb.TRANSACTION_HEADER_ID_IN,
    fab.asset_type
  FROM (
    SELECT
      *
    FROM silver_bec_ods.fa_mc_books
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fb
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_mc_book_controls
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fbc
    ON fb.book_type_code = fbc.book_type_code
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_mc_deprn_detail
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fdd
    ON fb.book_type_code = fdd.book_type_code AND fb.asset_id = fdd.asset_id
  INNER JOIN (
    SELECT
      PERIOD_NAME,
      PERIOD_COUNTER,
      period_close_date,
      book_type_code,
      period_open_date
    FROM (
      SELECT
        *
      FROM silver_bec_ods.fa_mc_deprn_periods
      WHERE
        is_deleted_flg <> 'Y'
    )
  ) AS fdp
    ON fb.book_type_code = fdp.book_type_code
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_additions_b
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fab
    ON fb.asset_id = fab.asset_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_asset_history
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fah
    ON fab.asset_id = fah.asset_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_distribution_history
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fdh
    ON fdh.asset_id = fb.asset_id AND fdd.distribution_id = fdh.distribution_id
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_mc_deprn_summary
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fds
    ON fb.book_type_code = fds.book_type_code
    AND fb.asset_id = fds.asset_id
    AND fdd.period_counter = fds.period_counter
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_mc_deprn_periods
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fdp2
    ON fdp2.period_counter = fds.period_counter AND fdp2.book_type_code = fds.book_type_code
  INNER JOIN (
    SELECT
      *
    FROM silver_bec_ods.fa_mc_deprn_periods
    WHERE
      is_deleted_flg <> 'Y'
  ) AS fdp3
    ON fdp3.period_counter = fdd.period_counter AND fdp3.book_type_code = fdd.book_type_code
  WHERE
    fb.date_effective <= COALESCE(fdp.period_close_date, CURRENT_TIMESTAMP())
    AND COALESCE(fb.date_ineffective, CURRENT_TIMESTAMP()) >= COALESCE(fdp.period_close_date, CURRENT_TIMESTAMP())
    AND (
      fdh.date_effective <= COALESCE(fdp.period_close_date, CURRENT_TIMESTAMP())
      AND COALESCE(fdh.date_ineffective, CURRENT_TIMESTAMP()) >= COALESCE(fdp.period_close_date, CURRENT_TIMESTAMP())
      OR COALESCE(fdh.date_ineffective, CURRENT_TIMESTAMP()) >= fdp.period_open_date
      AND COALESCE(fdh.date_ineffective, CURRENT_TIMESTAMP()) <= fdp.period_close_date
    )
    AND fdd.period_counter = (
      SELECT
        MAX(period_counter)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fa_mc_deprn_detail
        WHERE
          is_deleted_flg <> 'Y'
      ) AS dd
      WHERE
        dd.book_type_code = fdp.book_type_code
        AND dd.asset_id = fb.asset_id
        AND dd.distribution_id = fdd.distribution_id
        AND dd.period_counter <= fdp.period_counter
    )
    AND fah.date_effective <= COALESCE(fdp.period_close_date, CURRENT_TIMESTAMP())
    AND COALESCE(fah.date_ineffective, CURRENT_TIMESTAMP()) >= COALESCE(fdp.period_close_date, CURRENT_TIMESTAMP())
    AND COALESCE(fb.period_counter_fully_retired, fdp.period_counter + 1) >= fdp.period_counter
) AS A;
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_fa_asset_list' AND batch_name = 'fa';