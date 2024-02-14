/* Delete Records */
DELETE FROM silver_bec_ods.MTL_ITEM_LOCATIONS
WHERE
  (COALESCE(INVENTORY_LOCATION_ID, 0), COALESCE(ORGANIZATION_ID, 0)) IN (
    SELECT
      COALESCE(stg.INVENTORY_LOCATION_ID, 0) AS INVENTORY_LOCATION_ID,
      COALESCE(stg.ORGANIZATION_ID, 0) AS ORGANIZATION_ID
    FROM silver_bec_ods.MTL_ITEM_LOCATIONS AS ods, bronze_bec_ods_stg.MTL_ITEM_LOCATIONS AS stg
    WHERE
      COALESCE(ods.INVENTORY_LOCATION_ID, 0) = COALESCE(stg.INVENTORY_LOCATION_ID, 0)
      AND COALESCE(ods.ORGANIZATION_ID, 0) = COALESCE(stg.ORGANIZATION_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_ITEM_LOCATIONS (
  inventory_location_id,
  inv_dock_door_id,
  inv_org_id,
  organization_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  description,
  descriptive_text,
  disable_date,
  inventory_location_type,
  picking_order,
  physical_location_code,
  location_maximum_units,
  subinventory_code,
  location_weight_uom_code,
  max_weight,
  volume_uom_code,
  max_cubic_area,
  x_coordinate,
  y_coordinate,
  z_coordinate,
  inventory_account_id,
  segment1,
  segment2,
  segment3,
  segment4,
  segment5,
  segment6,
  segment7,
  segment8,
  segment9,
  segment10,
  segment11,
  segment12,
  segment13,
  segment14,
  segment15,
  segment16,
  segment17,
  segment18,
  segment19,
  segment20,
  summary_flag,
  enabled_flag,
  start_date_active,
  end_date_active,
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
  project_id,
  task_id,
  physical_location_id,
  pick_uom_code,
  dimension_uom_code,
  length,
  width,
  height,
  locator_status,
  status_id,
  current_cubic_area,
  available_cubic_area,
  current_weight,
  available_weight,
  location_current_units,
  location_available_units,
  inventory_item_id,
  suggested_cubic_area,
  suggested_weight,
  location_suggested_units,
  empty_flag,
  mixed_items_flag,
  dropping_order,
  availability_type,
  inventory_atp_code,
  reservable_type,
  alias,
  AREA_ROW_ID,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    inventory_location_id,
    inv_dock_door_id,
    inv_org_id,
    organization_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    description,
    descriptive_text,
    disable_date,
    inventory_location_type,
    picking_order,
    physical_location_code,
    location_maximum_units,
    subinventory_code,
    location_weight_uom_code,
    max_weight,
    volume_uom_code,
    max_cubic_area,
    x_coordinate,
    y_coordinate,
    z_coordinate,
    inventory_account_id,
    segment1,
    segment2,
    segment3,
    segment4,
    segment5,
    segment6,
    segment7,
    segment8,
    segment9,
    segment10,
    segment11,
    segment12,
    segment13,
    segment14,
    segment15,
    segment16,
    segment17,
    segment18,
    segment19,
    segment20,
    summary_flag,
    enabled_flag,
    start_date_active,
    end_date_active,
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
    project_id,
    task_id,
    physical_location_id,
    pick_uom_code,
    dimension_uom_code,
    length,
    width,
    height,
    locator_status,
    status_id,
    current_cubic_area,
    available_cubic_area,
    current_weight,
    available_weight,
    location_current_units,
    location_available_units,
    inventory_item_id,
    suggested_cubic_area,
    suggested_weight,
    location_suggested_units,
    empty_flag,
    mixed_items_flag,
    dropping_order,
    availability_type,
    inventory_atp_code,
    reservable_type,
    alias,
    AREA_ROW_ID,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_ITEM_LOCATIONS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(INVENTORY_LOCATION_ID, 0), COALESCE(ORGANIZATION_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(INVENTORY_LOCATION_ID, 0) AS INVENTORY_LOCATION_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MTL_ITEM_LOCATIONS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(INVENTORY_LOCATION_ID, 0),
        COALESCE(ORGANIZATION_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_ITEM_LOCATIONS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_ITEM_LOCATIONS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(INVENTORY_LOCATION_ID, 0), COALESCE(ORGANIZATION_ID, 0)) IN (
    SELECT
      COALESCE(INVENTORY_LOCATION_ID, 0),
      COALESCE(ORGANIZATION_ID, 0)
    FROM bec_raw_dl_ext.MTL_ITEM_LOCATIONS
    WHERE
      (COALESCE(INVENTORY_LOCATION_ID, 0), COALESCE(ORGANIZATION_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(INVENTORY_LOCATION_ID, 0),
          COALESCE(ORGANIZATION_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_ITEM_LOCATIONS
        GROUP BY
          COALESCE(INVENTORY_LOCATION_ID, 0),
          COALESCE(ORGANIZATION_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_item_locations';