DROP table IF EXISTS gold_bec_dwh.FACT_RAPA_RECEIPT_FORECAST;
CREATE TABLE gold_bec_dwh.FACT_RAPA_RECEIPT_FORECAST AS
(
  SELECT
    inventory_item_id,
    organization_id,
    cost_type,
    plan_name,
    data_type,
    order_group,
    order_type_text,
    po_number,
    new_dock_date1,
    aging_period,
    dock_promised_date,
    part_number,
    description,
    planning_make_buy_code,
    category_name,
    quantity,
    primary_quantity,
    unit_cost,
    primary_uom_code,
    extended_cost,
    po_line_type,
    vendor_name,
    mtl_cost,
    ext_mtl_cost,
    variance,
    source_organization_id,
    planner_code,
    buyer_name,
    transactional_uom_code,
    release_num,
    inventory_item_id_key,
    organization_id_key,
    source_organization_id_key,
    is_deleted_flg,
    source_app_id,
    dw_load_id,
    dw_insert_date,
    dw_update_date
  FROM gold_bec_dwh.FACT_RAPA_CVMI_ORDERS_TMP1
  UNION ALL
  SELECT
    inventory_item_id,
    organization_id,
    cost_type,
    plan_name,
    data_type,
    order_group,
    order_type_text,
    po_number,
    promised_date1,
    aging_period,
    promised_date,
    purchase_item,
    item_description,
    planning_make_buy_code,
    category_name,
    po_open_qty,
    primary_quantity,
    unit_price,
    primary_unit_of_measure,
    po_open_amount,
    po_line_type,
    vendor_name,
    mtl_cost,
    ext_mtl_cost,
    variance,
    source_organization_id,
    planner_code,
    buyer_name,
    transactional_uom_code,
    release_num,
    inventory_item_id_key,
    organization_id_key,
    source_organization_id_key,
    is_deleted_flg,
    source_app_id,
    dw_load_id,
    dw_insert_date,
    dw_update_date
  FROM gold_bec_dwh.FACT_RAPA_PO_TMP2
  UNION ALL
  SELECT
    inventory_item_id,
    organization_id,
    cost_type,
    plan_name,
    data_type,
    order_group,
    order_type_text,
    requisition_number,
    need_by_date,
    aging_period,
    dock_promised_date,
    part_number,
    description,
    planning_make_buy_code,
    category_name,
    so_ship_qty,
    primary_quantity,
    unit_price,
    primary_unit_of_measure,
    extended_cost,
    po_line_type,
    vendor_name,
    std_cost,
    ext_std_cost,
    variance,
    source_organization_id,
    planner_code,
    buyer_name,
    transactional_uom_code,
    release_num,
    inventory_item_id_key,
    organization_id_key,
    source_organization_id_key,
    is_deleted_flg,
    source_app_id,
    dw_load_id,
    dw_insert_date,
    dw_update_date
  FROM gold_bec_dwh.FACT_RAPA_POREQ_TMP3
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_rapa_receipt_forecast' AND batch_name = 'ascp';