DROP table IF EXISTS gold_bec_dwh.DIM_FA_ASSET_CATEGORY;
CREATE TABLE gold_bec_dwh.DIM_FA_ASSET_CATEGORY AS
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
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_fa_asset_category' AND batch_name = 'fa';