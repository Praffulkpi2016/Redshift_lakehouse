/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_FA_ASSET
WHERE
  (COALESCE(asset_id, 0), COALESCE(book_type_code, 'NA'), COALESCE(period_retired_name, 'NA')) IN (
    SELECT
      COALESCE(ods.asset_id, 0) AS asset_id,
      COALESCE(ods.book_type_code, 'NA') AS book_type_code,
      COALESCE(ods.period_name, 'NA') AS period_retired_name
    FROM gold_bec_dwh.DIM_FA_ASSET AS dw, (
      SELECT
        fa_additions_b.asset_id,
        fa_books.book_type_code,
        fa_deprn_periods.period_name
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
      WHERE
        (
          fa_additions_b.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_fa_asset' AND batch_name = 'fa'
          )
          OR fa_books.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_fa_asset' AND batch_name = 'fa'
          )
          OR fa_deprn_periods.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_fa_asset' AND batch_name = 'fa'
          )
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.asset_id, 0) || '-' || COALESCE(ods.book_type_code, 'NA') || '-' || COALESCE(ods.period_name, 'NA')
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_FA_ASSET (
  asset_id,
  book_type_code,
  asset_number,
  asset_key_ccid,
  asset_type,
  tag_number,
  parent_asset_id,
  manufacturer_name,
  serial_number,
  model_number,
  property_type_code,
  in_use_flag,
  owned_leased,
  new_used,
  additions_b_last_upd_dt,
  last_updated_by,
  created_by,
  creation_date,
  asset_description,
  additions_tl_last_upd_dt,
  asset_major_category,
  asset_minor_category,
  date_placed_in_service,
  x_custom,
  life_in_months,
  deprn_method,
  grp_asset_id,
  asset_subtype,
  current_units,
  asset_category,
  asset_category_desc,
  depreciate_flg,
  cat_b_last_upd_dt,
  cat_tl_last_upd_dt,
  books_last_upd_dt,
  period_retired_name,
  original_cost,
  PRODUCTION_CAPACITY,
  ADJUSTED_RATE,
  GL_ACCOUNT,
  RESERVE_ACCOUNT,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
  WHERE
    (
      fa_additions_b.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_fa_asset' AND batch_name = 'fa'
      )
      OR fa_books.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_fa_asset' AND batch_name = 'fa'
      )
      OR fa_deprn_periods.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_fa_asset' AND batch_name = 'fa'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_FA_ASSET SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(asset_id, 0), COALESCE(book_type_code, 'NA'), COALESCE(period_retired_name, 'NA')) IN (
    SELECT
      COALESCE(ods.asset_id, 0) AS asset_id,
      COALESCE(ods.book_type_code, 'NA') AS book_type_code,
      COALESCE(ods.period_name, 'NA') AS period_retired_name
    FROM gold_bec_dwh.DIM_FA_ASSET AS dw, (
      SELECT
        fa_additions_b.asset_id,
        fa_books.book_type_code,
        fa_deprn_periods.period_name
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fa_additions_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fa_additions_b
      LEFT JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_additions_tl
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fa_additions_tl
        ON fa_additions_b.asset_id = fa_additions_tl.asset_id
        AND fa_additions_tl.language = 'US'
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_categories_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fa_categories_b
        ON fa_categories_b.category_id = fa_additions_b.asset_category_id
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_books
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fa_books
        ON fa_books.asset_id = fa_additions_b.asset_id
        AND fa_books.date_ineffective IS NULL /* added to consider only active rows	*/
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_categories_tl
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fa_categories_tl
        ON fa_categories_tl.category_id = fa_additions_b.asset_category_id
        AND fa_categories_tl.language = 'US'
      LEFT JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_deprn_periods
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fa_deprn_periods
        ON fa_deprn_periods.period_counter = fa_books.period_counter_fully_retired
        AND fa_deprn_periods.book_type_code = fa_books.book_type_code
      /* added tables */
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_category_books
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fa_category_books
        ON fa_category_books.book_type_code = fa_books.book_type_code
        AND fa_category_books.CATEGORY_ID = fa_additions_b.asset_category_id
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.asset_id, 0) || '-' || COALESCE(ods.book_type_code, 'NA') || '-' || COALESCE(ods.period_name, 'NA')
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_fa_asset' AND batch_name = 'fa';