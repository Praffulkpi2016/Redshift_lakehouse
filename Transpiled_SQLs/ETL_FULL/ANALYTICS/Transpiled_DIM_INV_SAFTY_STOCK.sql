DROP table IF EXISTS gold_bec_dwh.DIM_INV_SAFTY_STOCK;
CREATE TABLE gold_bec_dwh.DIM_INV_SAFTY_STOCK AS
(
  SELECT
    organization_id,
    inventory_item_id, /* ,plan_id */
    safety_stock_quantity,
    effectivity_date,
    safety_stock_code,
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
    ) || '-' || COALESCE(INVENTORY_ITEM_ID, 0) || '-' || COALESCE(ORGANIZATION_ID, 0) || '-' || COALESCE(EFFECTIVITY_DATE, '1900-01-01 12:00:00') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.mtl_safety_stocks
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_inv_safty_stock' AND batch_name = 'inv';