DROP table IF EXISTS gold_bec_dwh.DIM_MSC_SAFTY_STOCK;
CREATE TABLE gold_bec_dwh.DIM_MSC_SAFTY_STOCK AS
(
  SELECT
    m.organization_id,
    m.inventory_item_id,
    s.plan_id,
    s.safety_stock_quantity,
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
    ) || '-' || COALESCE(m.INVENTORY_ITEM_ID, 0) || '-' || COALESCE(m.ORGANIZATION_ID, 0) || '-' || COALESCE(s.PLAN_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.MSC_SAFETY_STOCKS AS s, silver_bec_ods.MSC_ITEMS AS i, silver_bec_ods.mtl_system_items_b AS m
  WHERE
    m.segment1 = i.item_name
    AND m.organization_id = s.organization_id
    AND s.inventory_item_id = i.inventory_item_id
    AND s.period_start_date = (
      SELECT
        MIN(Period_start_date)
      FROM silver_bec_ods.MSC_SAFETY_STOCKS
      WHERE
        organization_id = s.organization_id
        AND inventory_item_id = s.inventory_item_id
        AND plan_id = s.plan_id
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_msc_safty_stock' AND batch_name = 'ascp';