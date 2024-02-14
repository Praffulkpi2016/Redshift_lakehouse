DROP TABLE IF EXISTS silver_bec_ods.MSC_ORDERS_V;
CREATE TABLE IF NOT EXISTS silver_bec_ods.MSC_ORDERS_V (
  source_table STRING,
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  inventory_item_id DECIMAL(15, 0),
  organization_id DECIMAL(15, 0),
  organization_code STRING,
  sr_instance_id DECIMAL(15, 0),
  plan_id DECIMAL(15, 0),
  compile_designator STRING,
  `action` STRING,
  new_due_date TIMESTAMP,
  old_due_date TIMESTAMP,
  new_start_date TIMESTAMP,
  order_number STRING,
  order_type DECIMAL(15, 0),
  order_type_text STRING,
  quantity_rate DECIMAL(28, 10),
  new_order_date TIMESTAMP,
  rescheduled_flag DECIMAL(15, 0),
  new_processing_days DECIMAL(15, 0),
  new_dock_date TIMESTAMP,
  item_segments STRING,
  planner_code STRING,
  build_in_wip_flag DECIMAL(15, 0),
  purchasing_enabled_flag DECIMAL(15, 0),
  planning_make_buy_code DECIMAL(15, 0),
  days_from_today DECIMAL(15, 0),
  wip_supply_type DECIMAL(15, 0),
  source_organization_id DECIMAL(15, 0),
  full_pegging DECIMAL(28, 10),
  source_vendor_name STRING,
  source_vendor_site_code STRING,
  supplier_name STRING,
  schedule_compression_days DECIMAL(15, 0),
  release_time_fence_code DECIMAL(28, 10),
  buyer_name STRING,
  description STRING,
  category_set_id DECIMAL(15, 0),
  category_name STRING,
  promise_date TIMESTAMP,
  request_date TIMESTAMP,
  customer_id DECIMAL(15, 0),
  customer_name STRING,
  lot_number STRING,
  subinventory_code STRING,
  need_by_date TIMESTAMP,
  list_price DECIMAL(28, 10),
  standard_cost DECIMAL(28, 10),
  ship_date TIMESTAMP,
  quantity DECIMAL(28, 10),
  po_line_id DECIMAL(15, 0),
  reschedule_days DECIMAL(15, 0),
  old_need_by_date TIMESTAMP,
  orders_days_late DECIMAL(15, 0),
  shipment_id DECIMAL(15, 0),
  receiving_calendar STRING,
  intransit_lead_time DECIMAL(28, 10),
  vmi_flag DECIMAL(15, 0),
  fill_kill_flag DECIMAL(15, 0),
  within_rel_time_fence DECIMAL(28, 10),
  amount DECIMAL(28, 10),
  so_line_split DECIMAL(28, 10),
  delivery_price DECIMAL(28, 10),
  comments STRING,
  transaction_id DECIMAL(15, 0),
  using_assembly_segments STRING,
  category_id DECIMAL(15, 0),
  using_assembly_item_id DECIMAL(15, 0),
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.MSC_ORDERS_V (
  source_table,
  creation_date,
  created_by,
  inventory_item_id,
  organization_id,
  organization_code,
  sr_instance_id,
  plan_id,
  compile_designator,
  `action`,
  new_due_date,
  old_due_date,
  new_start_date,
  order_number,
  order_type,
  order_type_text,
  quantity_rate,
  new_order_date,
  rescheduled_flag,
  new_processing_days,
  new_dock_date,
  item_segments,
  planner_code,
  build_in_wip_flag,
  purchasing_enabled_flag,
  planning_make_buy_code,
  days_from_today,
  wip_supply_type,
  source_organization_id,
  full_pegging,
  source_vendor_name,
  source_vendor_site_code,
  supplier_name,
  schedule_compression_days,
  release_time_fence_code,
  buyer_name,
  description,
  category_set_id,
  category_name,
  promise_date,
  request_date,
  customer_id,
  customer_name,
  lot_number,
  subinventory_code,
  need_by_date,
  list_price,
  standard_cost,
  ship_date,
  quantity,
  po_line_id,
  reschedule_days,
  old_need_by_date,
  orders_days_late,
  shipment_id,
  receiving_calendar,
  intransit_lead_time,
  vmi_flag,
  fill_kill_flag,
  within_rel_time_fence,
  amount,
  so_line_split,
  delivery_price,
  comments,
  transaction_id,
  using_assembly_segments,
  category_id,
  using_assembly_item_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  source_table,
  creation_date,
  created_by,
  inventory_item_id,
  organization_id,
  organization_code,
  sr_instance_id,
  plan_id,
  compile_designator,
  `action`,
  new_due_date,
  old_due_date,
  new_start_date,
  order_number,
  order_type,
  order_type_text,
  quantity_rate,
  new_order_date,
  rescheduled_flag,
  new_processing_days,
  new_dock_date,
  item_segments,
  planner_code,
  build_in_wip_flag,
  purchasing_enabled_flag,
  planning_make_buy_code,
  days_from_today,
  wip_supply_type,
  source_organization_id,
  full_pegging,
  source_vendor_name,
  source_vendor_site_code,
  supplier_name,
  schedule_compression_days,
  release_time_fence_code,
  buyer_name,
  description,
  category_set_id,
  category_name,
  promise_date,
  request_date,
  customer_id,
  customer_name,
  lot_number,
  subinventory_code,
  need_by_date,
  list_price,
  standard_cost,
  ship_date,
  quantity,
  po_line_id,
  reschedule_days,
  old_need_by_date,
  orders_days_late,
  shipment_id,
  receiving_calendar,
  intransit_lead_time,
  vmi_flag,
  fill_kill_flag,
  within_rel_time_fence,
  amount,
  so_line_split,
  delivery_price,
  comments,
  transaction_id,
  using_assembly_segments,
  category_id,
  using_assembly_item_id,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.MSC_ORDERS_V;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_orders_v';