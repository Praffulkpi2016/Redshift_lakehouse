TRUNCATE table gold_bec_dwh.FACT_FA_ASSETS_ADDITION;
INSERT INTO gold_bec_dwh.FACT_FA_ASSETS_ADDITION
SELECT
  TRANSACTION_HEADER_ID,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || TRANSACTION_HEADER_ID AS TRANSACTION_HEADER_ID_KEY,
  TRANSACTION_TYPE_CODE,
  TRANSACTION_DATE_ENTERED,
  INVOICE_TRANSACTION_ID,
  MASS_REFERENCE_ID,
  LAST_UPDATE_DATE1,
  DATE_EFFECTIVE,
  SOURCE_TYPE_CODE,
  ADJUSTMENT_TYPE,
  DEBIT_CREDIT_FLAG,
  BOOK_TYPE_CODE,
  ASSET_ID,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || ASSET_ID AS ASSET_ID_KEY,
  ADJUSTMENT_AMOUNT,
  DISTRIBUTION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  ADJUSTMENT_LINE_ID,
  ASSET_INVOICE_ID,
  SOURCE_DEST_CODE,
  PERIOD_COUNTER_CREATED,
  CODE_COMBINATION_ID,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || CODE_COMBINATION_ID AS CODE_COMBINATION_ID_KEY,
  LOCATION_ID,
  LAST_UPDATE_DATE2,
  UNITS_ASSIGNED,
  DATE_INEFFECTIVE,
  DATE_PLACED_IN_SERVICE,
  LIFE_IN_MONTHS,
  DEPRN_METHOD,
  GRP_ASSET_ID,
  PROD,
  ADJ_RATE,
  BONUS_RATE,
  ASSET_TYPE,
  GL_ACCOUNT,
  RESERVE_ACCT,
  COST,
  YTD_DEPRN,
  DEPRN_RESERVE,
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
  ) || '-' || COALESCE(TRANSACTION_HEADER_ID, 0) || '-' || COALESCE(DISTRIBUTION_ID, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    th.transaction_header_id,
    th.transaction_type_code,
    th.transaction_date_entered,
    th.invoice_transaction_id,
    th.mass_reference_id,
    th.last_update_date AS last_update_date1,
    th.date_effective,
    adj1.source_type_code,
    adj1.adjustment_type,
    adj1.debit_credit_flag,
    adj1.book_type_code,
    adj1.asset_id,
    adj1.adjustment_amount,
    adj1.distribution_id,
    adj1.last_update_date,
    adj1.last_updated_by,
    adj1.adjustment_line_id,
    adj1.asset_invoice_id,
    adj1.source_dest_code AS source_dest_code,
    adj1.period_counter_created,
    dh.code_combination_id,
    dh.location_id,
    dh.last_update_date AS last_update_date2,
    dh.units_assigned AS units_assigned,
    COALESCE(dh.date_ineffective, TO_DATE('01/01/9999 00:00:00', 'mm/dd/yyyy hh24:mi:ss')) AS date_ineffective,
    bks.date_placed_in_service AS date_placed_in_service,
    bks.life_in_months AS life_in_months,
    bks.deprn_method_code AS deprn_method,
    bks.group_asset_id AS grp_asset_id,
    bks.PRODUCTION_CAPACITY AS PROD,
    bks.ADJUSTED_RATE AS ADJ_RATE,
    CASE WHEN AH.ASSET_TYPE = 'CIP' THEN 0 ELSE COALESCE(DS.BONUS_RATE, 0) END AS BONUS_RATE,
    AH.ASSET_TYPE AS ASSET_TYPE,
    CASE WHEN AH.ASSET_TYPE = 'CIP' THEN fcb.CIP_COST_ACCT ELSE fcb.ASSET_COST_ACCT END AS GL_ACCOUNT,
    CASE WHEN AH.ASSET_TYPE = 'CIP' THEN NULL ELSE fcb.DEPRN_RESERVE_ACCT END AS RESERVE_ACCT,
    COALESCE(
      CASE WHEN adj1.debit_credit_flag = 'DR' THEN 1 ELSE -1 END * adj1.ADJUSTMENT_AMOUNT,
      DD.ADDITION_COST_TO_CLEAR
    ) AS cost,
    COALESCE(DD.YTD_DEPRN, 0) AS YTD_DEPRN,
    DD.DEPRN_RESERVE AS DEPRN_RESERVE
  FROM (
    SELECT
      *
    FROM silver_bec_ods.fa_asset_history
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ah, (
    SELECT
      *
    FROM silver_bec_ods.fa_transaction_headers
    WHERE
      is_deleted_flg <> 'Y'
  ) AS th, (
    SELECT
      *
    FROM silver_bec_ods.fa_distribution_history
    WHERE
      is_deleted_flg <> 'Y'
  ) AS dh, (
    SELECT
      *
    FROM silver_bec_ods.FA_ADJUSTMENTS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ADJ1, (
    SELECT
      *
    FROM silver_bec_ods.fa_books
    WHERE
      is_deleted_flg <> 'Y'
  ) AS bks, (
    SELECT
      *
    FROM silver_bec_ods.fa_deprn_summary
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ds, (
    SELECT
      *
    FROM silver_bec_ods.FA_DEPRN_DETAIL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS DD, silver_bec_ods.fa_category_books AS fcb
  WHERE
    1 = 1
    AND adj1.book_type_code = th.book_type_code
    AND adj1.transaction_header_id = th.transaction_header_id
    AND (
      (
        adj1.source_type_code = 'CIP ADDITION' AND adj1.adjustment_type = 'CIP COST'
      )
      OR (
        adj1.source_type_code = 'ADDITION' AND adj1.adjustment_type = 'COST'
      )
    )
    AND dh.distribution_id = adj1.distribution_id
    AND ah.asset_id = th.asset_id
    AND th.date_effective >= ah.date_effective
    AND th.date_effective < COALESCE(ah.date_ineffective, CURRENT_TIMESTAMP())
    AND bks.transaction_header_id_in = th.transaction_header_id
    AND dd.BOOK_TYPE_CODE() = adj1.book_type_code
    AND dd.DISTRIBUTION_ID() = adj1.distribution_id
    AND dd.DEPRN_SOURCE_CODE() = 'B'
    AND ds.BOOK_TYPE_CODE() = adj1.book_type_code
    AND ds.ASSET_ID() = adj1.asset_id
    AND ds.PERIOD_COUNTER() = adj1.period_counter_created
    AND fcb.CATEGORY_ID = AH.CATEGORY_ID
    AND fcb.BOOK_TYPE_CODE = TH.BOOK_TYPE_CODE
  UNION
  SELECT
    th.transaction_header_id,
    th.transaction_type_code,
    th.transaction_date_entered,
    th.invoice_transaction_id,
    th.mass_reference_id,
    th.last_update_date AS last_update_date1,
    th.date_effective,
    NULL AS source_type_code,
    NULL AS adjustment_type,
    NULL AS debit_credit_flag,
    th.book_type_code,
    th.asset_id,
    NULL AS adjustment_amount,
    NULL AS distribution_id,
    NULL AS last_update_date,
    NULL AS last_updated_by,
    NULL AS adjustment_line_id,
    NULL AS asset_invoice_id,
    NULL AS source_dest_code,
    NULL AS period_counter_created,
    dh.code_combination_id,
    dh.location_id,
    dh.last_update_date AS last_update_date2,
    dh.units_assigned AS units_assigned,
    COALESCE(dh.date_ineffective, TO_DATE('01/01/9999 00:00:00', 'mm/dd/yyyy hh24:mi:ss')) AS date_ineffective,
    bks.date_placed_in_service AS date_placed_in_service,
    bks.life_in_months AS life_in_months,
    bks.deprn_method_code AS deprn_method,
    bks.group_asset_id AS grp_asset_id,
    bks.PRODUCTION_CAPACITY AS PROD,
    bks.ADJUSTED_RATE AS ADJ_RATE,
    CASE WHEN AH.ASSET_TYPE = 'CIP' THEN 0 ELSE COALESCE(DS.BONUS_RATE, 0) END AS BONUS_RATE,
    AH.ASSET_TYPE AS ASSET_TYPE,
    CASE WHEN AH.ASSET_TYPE = 'CIP' THEN fcb.CIP_COST_ACCT ELSE fcb.ASSET_COST_ACCT END AS GL_ACCOUNT,
    CASE WHEN AH.ASSET_TYPE = 'CIP' THEN NULL ELSE fcb.DEPRN_RESERVE_ACCT END AS RESERVE_ACCT,
    0 AS cost,
    0 AS YTD_DEPRN,
    0 AS DEPRN_RESERVE
  FROM (
    SELECT
      *
    FROM silver_bec_ods.fa_asset_history
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ah, (
    SELECT
      *
    FROM silver_bec_ods.fa_distribution_history
    WHERE
      is_deleted_flg <> 'Y'
  ) AS dh, (
    SELECT
      *
    FROM silver_bec_ods.fa_books
    WHERE
      is_deleted_flg <> 'Y'
  ) AS bks, (
    SELECT
      *
    FROM silver_bec_ods.FA_DEPRN_SUMMARY
    WHERE
      is_deleted_flg <> 'Y'
  ) AS DS, (
    SELECT
      th.book_type_code,
      th.transaction_header_id,
      th.asset_id,
      th.date_effective,
      dp.period_counter,
      th.transaction_type_code,
      th.transaction_date_entered,
      th.invoice_transaction_id,
      th.mass_reference_id,
      th.last_update_date
    FROM (
      SELECT
        *
      FROM silver_bec_ods.fa_transaction_headers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS th, (
      SELECT
        *
      FROM silver_bec_ods.fa_deprn_periods
      WHERE
        is_deleted_flg <> 'Y'
    ) AS dp
    WHERE
      th.transaction_type_code IN ('ADDITION', 'CIP ADDITION')
      AND dp.book_type_code = th.book_type_code
      AND th.date_effective BETWEEN dp.period_open_date AND COALESCE(dp.period_close_date, CURRENT_TIMESTAMP())
  ) AS th, silver_bec_ods.fa_category_books AS fcb
  WHERE
    dh.asset_id = th.asset_id
    AND th.date_effective >= dh.date_effective
    AND th.date_effective < COALESCE(dh.date_ineffective, CURRENT_TIMESTAMP())
    AND ah.asset_id = th.asset_id
    AND th.date_effective >= ah.date_effective
    AND th.date_effective < COALESCE(ah.date_ineffective, CURRENT_TIMESTAMP())
    AND bks.transaction_header_id_in = th.transaction_header_id
    AND bks.cost = 0
    AND ds.BOOK_TYPE_CODE() = th.book_type_code
    AND ds.ASSET_ID() = th.asset_id
    AND ds.PERIOD_COUNTER() = th.period_counter
    AND fcb.CATEGORY_ID = AH.CATEGORY_ID
    AND fcb.BOOK_TYPE_CODE = TH.BOOK_TYPE_CODE
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_fa_assets_addition' AND batch_name = 'fa';