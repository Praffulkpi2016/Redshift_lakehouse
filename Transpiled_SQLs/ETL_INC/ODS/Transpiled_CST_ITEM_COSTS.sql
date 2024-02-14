/* Delete Records */
DELETE FROM silver_bec_ods.CST_ITEM_COSTS
WHERE
  (COST_TYPE_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID) IN (
    SELECT
      stg.COST_TYPE_ID,
      stg.INVENTORY_ITEM_ID,
      stg.ORGANIZATION_ID
    FROM silver_bec_ods.CST_ITEM_COSTS AS ods, bronze_bec_ods_stg.CST_ITEM_COSTS AS stg
    WHERE
      ods.COST_TYPE_ID = stg.COST_TYPE_ID
      AND ods.INVENTORY_ITEM_ID = stg.INVENTORY_ITEM_ID
      AND ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.CST_ITEM_COSTS (
  inventory_item_id,
  organization_id,
  cost_type_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  inventory_asset_flag,
  lot_size,
  based_on_rollup_flag,
  shrinkage_rate,
  defaulted_flag,
  cost_update_id,
  pl_material,
  pl_material_overhead,
  pl_resource,
  pl_outside_processing,
  pl_overhead,
  tl_material,
  tl_material_overhead,
  tl_resource,
  tl_outside_processing,
  tl_overhead,
  material_cost,
  material_overhead_cost,
  resource_cost,
  outside_processing_cost,
  overhead_cost,
  pl_item_cost,
  tl_item_cost,
  item_cost,
  unburdened_cost,
  burden_cost,
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
  rollup_id,
  assignment_set_id,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    inventory_item_id,
    organization_id,
    cost_type_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    inventory_asset_flag,
    lot_size,
    based_on_rollup_flag,
    shrinkage_rate,
    defaulted_flag,
    cost_update_id,
    pl_material,
    pl_material_overhead,
    pl_resource,
    pl_outside_processing,
    pl_overhead,
    tl_material,
    tl_material_overhead,
    tl_resource,
    tl_outside_processing,
    tl_overhead,
    material_cost,
    material_overhead_cost,
    resource_cost,
    outside_processing_cost,
    overhead_cost,
    pl_item_cost,
    tl_item_cost,
    item_cost,
    unburdened_cost,
    burden_cost,
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
    rollup_id,
    assignment_set_id,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.CST_ITEM_COSTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COST_TYPE_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, kca_seq_id) IN (
      SELECT
        COST_TYPE_ID,
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.CST_ITEM_COSTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COST_TYPE_ID,
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.CST_ITEM_COSTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.CST_ITEM_COSTS SET IS_DELETED_FLG = 'Y'
WHERE
  (COST_TYPE_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID) IN (
    SELECT
      COST_TYPE_ID,
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID
    FROM bec_raw_dl_ext.CST_ITEM_COSTS
    WHERE
      (COST_TYPE_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, KCA_SEQ_ID) IN (
        SELECT
          COST_TYPE_ID,
          INVENTORY_ITEM_ID,
          ORGANIZATION_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.CST_ITEM_COSTS
        GROUP BY
          COST_TYPE_ID,
          INVENTORY_ITEM_ID,
          ORGANIZATION_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'cst_item_costs';