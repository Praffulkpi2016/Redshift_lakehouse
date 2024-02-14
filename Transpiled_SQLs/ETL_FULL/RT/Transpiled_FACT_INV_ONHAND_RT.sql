DROP table IF EXISTS gold_bec_dwh_rpt.FACT_INV_ONHAND_RT;
CREATE TABLE gold_bec_dwh_rpt.FACT_INV_ONHAND_RT AS
(
  SELECT
    fio.*,
    diic.category_set_name,
    diic.category_set_desc,
    diic.item_category_segment1,
    diic.item_category_segment2,
    diic.item_category_segment3,
    diic.item_category_segment4,
    diic.item_category_desc,
    diss.plan_id,
    diss.safety_stock_quantity
  FROM gold_bec_dwh.FACT_INV_ONHAND AS fio, gold_bec_dwh.dim_inv_item_category_set AS diic, gold_bec_dwh.DIM_MSC_SAFTY_STOCK AS diss
  WHERE
    fio.inventory_item_id = diic.INVENTORY_ITEM_ID()
    AND fio.organization_id = diic.ORGANIZATION_ID()
    AND diic.CATEGORY_SET_ID() = 1
    AND fio.inventory_item_id = diss.INVENTORY_ITEM_ID()
    AND fio.organization_id = diss.ORGANIZATION_ID()
    AND diss.PLAN_ID() = 40029
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_inv_onhand_rt' AND batch_name = 'inv';