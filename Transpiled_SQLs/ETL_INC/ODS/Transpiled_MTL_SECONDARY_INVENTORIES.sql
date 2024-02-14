/* Delete Records */
DELETE FROM silver_bec_ods.MTL_SECONDARY_INVENTORIES
WHERE
  (SECONDARY_INVENTORY_NAME, ORGANIZATION_ID) IN (
    SELECT
      stg.SECONDARY_INVENTORY_NAME,
      stg.ORGANIZATION_ID
    FROM silver_bec_ods.MTL_SECONDARY_INVENTORIES AS ods, bronze_bec_ods_stg.MTL_SECONDARY_INVENTORIES AS stg
    WHERE
      ods.SECONDARY_INVENTORY_NAME = stg.SECONDARY_INVENTORY_NAME
      AND ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_SECONDARY_INVENTORIES (
  secondary_inventory_name,
  organization_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  description,
  disable_date,
  inventory_atp_code,
  availability_type,
  reservable_type,
  locator_type,
  picking_order,
  material_account,
  material_overhead_account,
  resource_account,
  overhead_account,
  outside_processing_account,
  quantity_tracked,
  asset_inventory,
  source_type,
  source_subinventory,
  source_organization_id,
  requisition_approval_type,
  expense_account,
  encumbrance_account,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  preprocessing_lead_time,
  processing_lead_time,
  postprocessing_lead_time,
  demand_class,
  project_id,
  task_id,
  subinventory_usage,
  notify_list_id,
  pick_uom_code,
  depreciable_flag,
  location_id,
  default_cost_group_id,
  status_id,
  default_loc_status_id,
  lpn_controlled_flag,
  pick_methodology,
  cartonization_flag,
  dropping_order,
  subinventory_type,
  planning_level,
  default_count_type_code,
  enable_bulk_pick,
  enable_locator_alias,
  enforce_alias_uniqueness,
  enable_opp_cyc_count,
  opp_cyc_count_days,
  opp_cyc_count_header_id,
  opp_cyc_count_quantity,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    secondary_inventory_name,
    organization_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    description,
    disable_date,
    inventory_atp_code,
    availability_type,
    reservable_type,
    locator_type,
    picking_order,
    material_account,
    material_overhead_account,
    resource_account,
    overhead_account,
    outside_processing_account,
    quantity_tracked,
    asset_inventory,
    source_type,
    source_subinventory,
    source_organization_id,
    requisition_approval_type,
    expense_account,
    encumbrance_account,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    preprocessing_lead_time,
    processing_lead_time,
    postprocessing_lead_time,
    demand_class,
    project_id,
    task_id,
    subinventory_usage,
    notify_list_id,
    pick_uom_code,
    depreciable_flag,
    location_id,
    default_cost_group_id,
    status_id,
    default_loc_status_id,
    lpn_controlled_flag,
    pick_methodology,
    cartonization_flag,
    dropping_order,
    subinventory_type,
    planning_level,
    default_count_type_code,
    enable_bulk_pick,
    enable_locator_alias,
    enforce_alias_uniqueness,
    enable_opp_cyc_count,
    opp_cyc_count_days,
    opp_cyc_count_header_id,
    opp_cyc_count_quantity,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_SECONDARY_INVENTORIES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (SECONDARY_INVENTORY_NAME, ORGANIZATION_ID, kca_seq_id) IN (
      SELECT
        SECONDARY_INVENTORY_NAME,
        ORGANIZATION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MTL_SECONDARY_INVENTORIES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        SECONDARY_INVENTORY_NAME,
        ORGANIZATION_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_SECONDARY_INVENTORIES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_SECONDARY_INVENTORIES SET IS_DELETED_FLG = 'Y'
WHERE
  (SECONDARY_INVENTORY_NAME, ORGANIZATION_ID) IN (
    SELECT
      SECONDARY_INVENTORY_NAME,
      ORGANIZATION_ID
    FROM bec_raw_dl_ext.MTL_SECONDARY_INVENTORIES
    WHERE
      (SECONDARY_INVENTORY_NAME, ORGANIZATION_ID, KCA_SEQ_ID) IN (
        SELECT
          SECONDARY_INVENTORY_NAME,
          ORGANIZATION_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_SECONDARY_INVENTORIES
        GROUP BY
          SECONDARY_INVENTORY_NAME,
          ORGANIZATION_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_secondary_inventories';