/* Delete Records */
DELETE FROM silver_bec_ods.XXBEC_MSC_ORDERS
WHERE
  (COALESCE(TRANSACTION_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0)) IN (
    SELECT
      COALESCE(stg.TRANSACTION_ID, 0) AS TRANSACTION_ID,
      COALESCE(stg.INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID
    FROM silver_bec_ods.XXBEC_MSC_ORDERS AS ods, bronze_bec_ods_stg.XXBEC_MSC_ORDERS AS stg
    WHERE
      COALESCE(ods.TRANSACTION_ID, 0) = COALESCE(stg.TRANSACTION_ID, 0)
      AND COALESCE(ods.INVENTORY_ITEM_ID, 0) = COALESCE(stg.INVENTORY_ITEM_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.XXBEC_MSC_ORDERS (
  plan_id,
  compile_designator,
  organization_code,
  organization_id,
  item_segments,
  requested_start_date,
  requested_completion_date,
  order_type_text,
  new_due_date,
  ship_set_name,
  arrival_set_name,
  schedule_arrival_date,
  schedule_ship_date,
  actual_start_date,
  planned_arrival_date,
  latest_acceptable_date,
  quantity_rate,
  qty_by_due_date,
  po_line_id,
  shipment_id,
  reschedule_days,
  old_due_date,
  sr_instance_id,
  disposition_id,
  description,
  original_item_name,
  prev_subst_item,
  prev_subst_org,
  original_item_qty,
  last_unit_completion_date,
  line_code,
  expiration_date,
  order_number,
  `action`,
  mrp_planning_code_text,
  old_order_quantity,
  orig_org_code,
  dest_org_code,
  dest_org_id,
  dest_inst_id,
  ship_method,
  orig_ship_method,
  source_vendor_name,
  source_vendor_site_code,
  planning_group,
  schedule_group_name,
  quantity,
  ship_date,
  assembly_demand_comp_date,
  planner_code,
  order_type,
  vendor_id,
  vendor_site_id,
  category_id,
  source_vendor_site_id,
  source_vendor_id,
  supplier_name,
  supplier_site_code,
  subinventory_code,
  mrp_planning_code,
  source_organization_id,
  status_code,
  customer_id,
  customer_name,
  ship_to_site_id,
  ship_to_site_name,
  customer_site_id,
  customer_site_name,
  promise_date,
  promise_ship_date,
  original_need_by_date,
  request_date,
  request_ship_date,
  category_set_id,
  sales_order_line_id,
  ship_set_id,
  customer_po_number,
  customer_po_line_number,
  buyer_name,
  source_table,
  transaction_id,
  INVENTORY_ITEM_ID,
  new_order_date,
  new_dock_date,
  new_start_date,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    plan_id,
    compile_designator,
    organization_code,
    organization_id,
    item_segments,
    requested_start_date,
    requested_completion_date,
    order_type_text,
    new_due_date,
    ship_set_name,
    arrival_set_name,
    schedule_arrival_date,
    schedule_ship_date,
    actual_start_date,
    planned_arrival_date,
    latest_acceptable_date,
    quantity_rate,
    qty_by_due_date,
    po_line_id,
    shipment_id,
    reschedule_days,
    old_due_date,
    sr_instance_id,
    disposition_id,
    description,
    original_item_name,
    prev_subst_item,
    prev_subst_org,
    original_item_qty,
    last_unit_completion_date,
    line_code,
    expiration_date,
    order_number,
    `action`,
    mrp_planning_code_text,
    old_order_quantity,
    orig_org_code,
    dest_org_code,
    dest_org_id,
    dest_inst_id,
    ship_method,
    orig_ship_method,
    source_vendor_name,
    source_vendor_site_code,
    planning_group,
    schedule_group_name,
    quantity,
    ship_date,
    assembly_demand_comp_date,
    planner_code,
    order_type,
    vendor_id,
    vendor_site_id,
    category_id,
    source_vendor_site_id,
    source_vendor_id,
    supplier_name,
    supplier_site_code,
    subinventory_code,
    mrp_planning_code,
    source_organization_id,
    status_code,
    customer_id,
    customer_name,
    ship_to_site_id,
    ship_to_site_name,
    customer_site_id,
    customer_site_name,
    promise_date,
    promise_ship_date,
    original_need_by_date,
    request_date,
    request_ship_date,
    category_set_id,
    sales_order_line_id,
    ship_set_id,
    customer_po_number,
    customer_po_line_number,
    buyer_name,
    source_table,
    transaction_id,
    INVENTORY_ITEM_ID,
    new_order_date,
    new_dock_date,
    new_start_date,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.XXBEC_MSC_ORDERS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(TRANSACTION_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(TRANSACTION_ID, 0) AS TRANSACTION_ID,
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.XXBEC_MSC_ORDERS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(TRANSACTION_ID, 0),
        COALESCE(INVENTORY_ITEM_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.XXBEC_MSC_ORDERS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.XXBEC_MSC_ORDERS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(TRANSACTION_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0)) IN (
    SELECT
      COALESCE(TRANSACTION_ID, 0),
      COALESCE(INVENTORY_ITEM_ID, 0)
    FROM bec_raw_dl_ext.XXBEC_MSC_ORDERS
    WHERE
      (COALESCE(TRANSACTION_ID, 0), COALESCE(INVENTORY_ITEM_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(TRANSACTION_ID, 0),
          COALESCE(INVENTORY_ITEM_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.XXBEC_MSC_ORDERS
        GROUP BY
          COALESCE(TRANSACTION_ID, 0),
          COALESCE(INVENTORY_ITEM_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'xxbec_msc_orders';