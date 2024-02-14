DROP table IF EXISTS gold_bec_dwh.DIM_FA_ASSET;
CREATE TABLE gold_bec_dwh.DIM_FA_ASSET AS
(
  SELECT
    fa_additions_b.asset_id,
    fa_books.book_type_code,
    fa_additions_b.asset_number,
    fa_additions_b.asset_key_ccid,
    fa_additions_b.asset_type,
    fa_additions_b.tag_number,
    fa_additions_b.parent_asset_id,
    fa_additions_b.manufacturer_name,
    fa_additions_b.serial_number,
    fa_additions_b.model_number,
    fa_additions_b.property_type_code,
    fa_additions_b.in_use_flag,
    fa_additions_b.owned_leased,
    fa_additions_b.new_used,
    fa_additions_b.last_update_date AS additions_b_last_upd_dt,
    fa_additions_b.last_updated_by,
    fa_additions_b.created_by,
    fa_additions_b.creation_date,
    fa_additions_tl.description AS asset_description,
    fa_additions_tl.last_update_date AS additions_tl_last_upd_dt,
    fa_categories_b.segment1 AS asset_major_category,
    fa_categories_b.segment2 AS asset_minor_category,
    fa_books.date_placed_in_service AS date_placed_in_service,
    '0' AS x_custom,
    fa_books.life_in_months AS life_in_months,
    fa_books.deprn_method_code AS deprn_method,
    fa_books.group_asset_id AS grp_asset_id,
    CASE
      WHEN (
        fa_additions_b.asset_type = 'GROUP' AND fa_books.group_asset_id IS NULL
      )
      THEN 'GROUP'
      WHEN (
        fa_additions_b.asset_type = 'CAPITALIZED' AND NOT fa_books.group_asset_id IS NULL
      )
      THEN 'MEMBER'
      WHEN (
        fa_additions_b.asset_type = 'CAPITALIZED' AND fa_books.group_asset_id IS NULL
      )
      THEN 'STANDALONE'
    END AS asset_subtype,
    fa_additions_b.current_units AS current_units,
    fa_additions_b.attribute_category_code AS asset_category,
    fa_categories_tl.description AS asset_category_desc,
    fa_books.depreciate_flag AS depreciate_flg,
    fa_categories_b.last_update_date AS cat_b_last_upd_dt,
    fa_categories_tl.last_update_date AS cat_tl_last_upd_dt,
    fa_books.last_update_date AS books_last_upd_dt,
    fa_deprn_periods.period_name AS period_retired_name,
    fa_books.original_cost,
    fa_books.PRODUCTION_CAPACITY,
    fa_books.ADJUSTED_RATE,
    CASE
      WHEN fa_additions_b.ASSET_TYPE = 'CIP'
      THEN fa_category_books.CIP_COST_ACCT
      ELSE fa_category_books.ASSET_COST_ACCT
    END AS GL_ACCOUNT,
    CASE
      WHEN fa_additions_b.ASSET_TYPE = 'CIP'
      THEN NULL
      ELSE fa_category_books.DEPRN_RESERVE_ACCT
    END AS RESERVE_ACCOUNT,
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
    ) || '-' || COALESCE(fa_additions_b.asset_id, 0) || '-' || COALESCE(fa_books.book_type_code, 'NA') || '-' || COALESCE(fa_deprn_periods.period_name, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.fa_additions_b AS fa_additions_b
  LEFT JOIN silver_bec_ods.fa_additions_tl AS fa_additions_tl
    ON fa_additions_b.asset_id = fa_additions_tl.asset_id
    AND fa_additions_tl.language = 'US'
  INNER JOIN silver_bec_ods.fa_categories_b AS fa_categories_b
    ON fa_categories_b.category_id = fa_additions_b.asset_category_id
  INNER JOIN silver_bec_ods.fa_books AS fa_books
    ON fa_books.asset_id = fa_additions_b.asset_id
    AND fa_books.date_ineffective IS NULL /* added to consider only active rows	*/
  INNER JOIN silver_bec_ods.fa_categories_tl AS fa_categories_tl
    ON fa_categories_tl.category_id = fa_additions_b.asset_category_id
    AND fa_categories_tl.language = 'US'
  LEFT JOIN silver_bec_ods.fa_deprn_periods AS fa_deprn_periods
    ON fa_deprn_periods.period_counter = fa_books.period_counter_fully_retired
    AND fa_deprn_periods.book_type_code = fa_books.book_type_code
  /* added tables */
  INNER JOIN silver_bec_ods.fa_category_books AS fa_category_books
    ON fa_category_books.book_type_code = fa_books.book_type_code
    AND fa_category_books.CATEGORY_ID = fa_additions_b.asset_category_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_fa_asset' AND batch_name = 'fa';