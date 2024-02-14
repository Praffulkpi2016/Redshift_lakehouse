DROP table IF EXISTS gold_bec_dwh.DIM_ITEM_STD_COSTS;
CREATE TABLE gold_bec_dwh.DIM_ITEM_STD_COSTS AS
(
  SELECT
    a.cost_update_id,
    a.inventory_item_id,
    a.organization_id,
    a.organization_id || '-' || a.inventory_item_id AS item_category_set1,
    a.organization_id || '-' || a.inventory_item_id AS item_category_set2,
    TRUNC(a.last_update_date) AS last_update_date,
    a.standard_cost,
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
    ) || '-' || COALESCE(a.COST_UPDATE_ID, 0) || '-' || COALESCE(a.INVENTORY_ITEM_ID, 0) || '-' || COALESCE(a.ORGANIZATION_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.cst_standard_costs AS a
  WHERE
    a.is_deleted_flg <> 'Y'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_item_std_costs' AND batch_name = 'po';