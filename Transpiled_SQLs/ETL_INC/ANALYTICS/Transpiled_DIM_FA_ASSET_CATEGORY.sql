/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_FA_ASSET_CATEGORY
WHERE
  COALESCE(FLOOR(category_id), 0) IN (
    SELECT
      COALESCE(FLOOR(ods.category_id), 0) AS category_id
    FROM gold_bec_dwh.DIM_FA_ASSET_CATEGORY AS dw, (
      SELECT
        fa_categories_b.category_id,
        fa_categories_b.last_update_date,
        fa_categories_b.kca_seq_id
      FROM silver_bec_ods.fa_categories_b AS fa_categories_b
      INNER JOIN silver_bec_ods.fa_categories_tl AS fa_categories_tl
        ON fa_categories_b.category_id = fa_categories_tl.category_id
      WHERE
        fa_categories_tl.language = 'US'
        AND (
          fa_categories_b.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'dim_fa_asset_category' AND batch_name = 'fa'
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
      ) || '-' || COALESCE(FLOOR(ods.category_id), 0)
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_FA_ASSET_CATEGORY (
  capitalize_flag,
  category_id,
  category_description,
  category_type,
  creation_date,
  created_by,
  enabled_flag,
  last_update_date,
  last_updated_by,
  owned_leased,
  property_1245_1250_code,
  property_type_code,
  segment1,
  segment2,
  segment3,
  segment4,
  segment5,
  segment6,
  segment7,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    fa_categories_b.capitalize_flag,
    fa_categories_b.category_id,
    fa_categories_tl.description AS category_description,
    fa_categories_b.category_type,
    fa_categories_b.creation_date,
    fa_categories_b.created_by,
    fa_categories_b.enabled_flag,
    fa_categories_b.last_update_date,
    fa_categories_b.last_updated_by,
    fa_categories_b.owned_leased,
    fa_categories_b.property_1245_1250_code,
    fa_categories_b.property_type_code,
    fa_categories_b.segment1,
    fa_categories_b.segment2,
    fa_categories_b.segment3,
    fa_categories_b.segment4,
    fa_categories_b.segment5,
    fa_categories_b.segment6,
    fa_categories_b.segment7,
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
    ) || '-' || COALESCE(FLOOR(fa_categories_b.category_id), 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.fa_categories_b AS fa_categories_b
  INNER JOIN silver_bec_ods.fa_categories_tl AS fa_categories_tl
    ON fa_categories_b.category_id = fa_categories_tl.category_id
  WHERE
    fa_categories_tl.language = 'US'
    AND (
      fa_categories_b.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_fa_asset_category' AND batch_name = 'fa'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_FA_ASSET_CATEGORY SET is_deleted_flg = 'Y'
WHERE
  NOT COALESCE(FLOOR(category_id), 0) IN (
    SELECT
      COALESCE(FLOOR(ods.category_id), 0) AS category_id
    FROM gold_bec_dwh.DIM_FA_ASSET_CATEGORY AS dw, (
      SELECT
        fa_categories_b.category_id
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fa_categories_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fa_categories_b
      INNER JOIN (
        SELECT
          *
        FROM silver_bec_ods.fa_categories_tl
        WHERE
          is_deleted_flg <> 'Y'
      ) AS fa_categories_tl
        ON fa_categories_b.category_id = fa_categories_tl.category_id
      WHERE
        fa_categories_tl.language = 'US'
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(FLOOR(ods.category_id), 0)
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_fa_asset_category' AND batch_name = 'fa';