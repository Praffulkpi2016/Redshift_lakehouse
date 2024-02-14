TRUNCATE table silver_bec_ods.MSC_SUP_DEM_ENTRIES_VMI_V;
INSERT INTO silver_bec_ods.MSC_SUP_DEM_ENTRIES_VMI_V (
  plan_id,
  sr_instance_id,
  customer_id,
  customer_site_id,
  customer_name,
  customer_site_name,
  supplier_id,
  supplier_site_id,
  supplier_name,
  supplier_site_name,
  inventory_item_id,
  item_name,
  description,
  order_details,
  planner_code,
  organization_id,
  vmi_type,
  aps_supplier_id,
  aps_supplier_site_id,
  aps_customer_id,
  aps_customer_site_id,
  buyer_code,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    plan_id,
    sr_instance_id,
    customer_id,
    customer_site_id,
    customer_name,
    customer_site_name,
    supplier_id,
    supplier_site_id,
    supplier_name,
    supplier_site_name,
    inventory_item_id,
    item_name,
    description,
    order_details,
    planner_code,
    organization_id,
    vmi_type,
    aps_supplier_id,
    aps_supplier_site_id,
    aps_customer_id,
    aps_customer_site_id,
    buyer_code,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MSC_SUP_DEM_ENTRIES_VMI_V
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_sup_dem_entries_vmi_v';