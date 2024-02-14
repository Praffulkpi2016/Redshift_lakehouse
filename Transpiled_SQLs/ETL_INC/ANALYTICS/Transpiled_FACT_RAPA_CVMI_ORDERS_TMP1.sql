TRUNCATE table gold_bec_dwh.FACT_RAPA_CVMI_ORDERS_TMP1;
INSERT INTO gold_bec_dwh.FACT_RAPA_CVMI_ORDERS_TMP1
(
  SELECT
    a.inventory_item_id,
    a.organization_id,
    a.cost_type,
    a.plan_name,
    a.data_type,
    a.order_group,
    a.order_type_text,
    a.po_number,
    a.new_dock_date1,
    a.aging_period,
    a.dock_promised_date,
    a.part_number,
    a.description,
    a.planning_make_buy_code,
    diics.item_category_segment1 || '.' || diics.item_category_segment2 AS category_name,
    CAST(a.quantity AS DECIMAL(38, 10)),
    a.primary_quantity,
    a.unit_cost,
    a.primary_uom_code,
    CAST(a.extended_cost AS DECIMAL(38, 10)),
    a.po_line_type,
    a.VENDOR_NAME,
    a.mtl_cost,
    CAST(a.ext_mtl_cost AS DECIMAL(38, 10)),
    CAST(a.variance AS DECIMAL(38, 10)),
    a.source_organization_id,
    a.planner_code,
    a.buyer_name,
    a.transactional_uom_code,
    a.release_num,
    a.inventory_item_id_KEY,
    a.organization_id_KEY,
    a.source_organization_id_KEY,
    a.is_deleted_flg,
    a.source_app_id,
    a.dw_load_id,
    a.dw_insert_date,
    a.dw_update_date
  FROM gold_bec_dwh.FACT_RAPA_STG1 AS a
  LEFT OUTER JOIN bec_Dwh.dim_inv_item_category_set AS diics
    ON a.inventory_item_id = diics.inventory_item_id
    AND a.organization_id = diics.organization_id
    AND diics.category_set_id = 1
  UNION ALL
  SELECT
    inventory_item_id,
    organization_id,
    cost_type,
    plan_name,
    data_type,
    order_group,
    order_type_text,
    order_number,
    new_dock_date1,
    aging_period,
    new_dock_date,
    item_segments,
    description,
    planning_make_buy_code,
    category_name,
    quantity,
    primary_quantity,
    calc_unit_cost,
    primary_unit_of_measure,
    CAST(ext_unit_cost AS DECIMAL(38, 10)),
    po_line_type,
    source_vendor_name,
    mtl_cost,
    CAST(ext_mtl_cost AS DECIMAL(38, 10)),
    CAST(variance AS DECIMAL(38, 10)),
    source_organization_id,
    planner_code,
    buyer_name,
    transactional_uom_code,
    release_num,
    inventory_item_id_KEY,
    organization_id_KEY,
    source_organization_id_KEY,
    is_deleted_flg,
    source_app_id,
    dw_load_id,
    dw_insert_date,
    dw_update_date
  FROM gold_bec_dwh.FACT_RAPA_STG2
  WHERE
    NOT ORDER_TYPE_TEXT IN ('CVMI Planned Order (Mat\'l Cost)', 'CVMI Planned Order', 'CVMI Shipment PO')
    AND source_organization_id IS NULL
  UNION ALL
  SELECT
    a.INVENTORY_ITEM_ID,
    a.organization_id,
    a.cost_type,
    a.plan_name,
    a.data_type,
    a.order_group,
    a.order_type_text,
    a.po_number,
    a.promised_date1 AS new_dock_date1,
    a.aging_period,
    a.promised_date,
    a.PART_NUMBER,
    a.description,
    a.planning_make_buy_code,
    diics.item_category_segment1 || '.' || diics.item_category_segment2 AS category_name,
    a.po_open_quantity,
    CAST(a.primary_quantity AS DECIMAL(38, 10)),
    a.price_override,
    a.primary_unit_of_measure,
    CAST(a.extended_cost AS DECIMAL(38, 10)),
    a.po_line_type,
    a.vendor_name,
    a.mtl_cost,
    CAST(a.ext_mtl_cost AS DECIMAL(38, 10)),
    CAST(a.variance AS DECIMAL(38, 10)),
    a.source_organization_id,
    a.planner_code,
    a.buyer_name,
    a.unit_meas_lookup_code,
    a.release_num,
    a.inventory_item_id_KEY,
    a.organization_id_KEY,
    a.source_organization_id_KEY,
    a.is_deleted_flg,
    a.source_app_id,
    a.dw_load_id,
    a.dw_insert_date,
    a.dw_update_date
  FROM gold_bec_dwh.FACT_RAPA_STG6 AS a
  LEFT OUTER JOIN bec_Dwh.dim_inv_item_category_set AS diics
    ON a.inventory_item_id = diics.inventory_item_id
    AND a.organization_id = diics.organization_id
    AND diics.category_set_id = 1
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_rapa_cvmi_orders_tmp1' AND batch_name = 'ascp';