DROP TABLE IF EXISTS silver_bec_ods.CST_ITEM_COSTS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.CST_ITEM_COSTS (
  inventory_item_id DECIMAL(15, 0),
  organization_id DECIMAL(15, 0),
  cost_type_id DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  inventory_asset_flag DECIMAL(15, 0),
  lot_size DECIMAL(15, 0),
  based_on_rollup_flag DECIMAL(15, 0),
  shrinkage_rate DECIMAL(28, 10),
  defaulted_flag DECIMAL(15, 0),
  cost_update_id DECIMAL(15, 0),
  pl_material DECIMAL(28, 10),
  pl_material_overhead DECIMAL(28, 10),
  pl_resource DECIMAL(28, 10),
  pl_outside_processing DECIMAL(28, 10),
  pl_overhead DECIMAL(28, 10),
  tl_material DECIMAL(28, 10),
  tl_material_overhead DECIMAL(28, 10),
  tl_resource DECIMAL(28, 10),
  tl_outside_processing DECIMAL(28, 10),
  tl_overhead DECIMAL(28, 10),
  material_cost DECIMAL(28, 10),
  material_overhead_cost DECIMAL(28, 10),
  resource_cost DECIMAL(28, 10),
  outside_processing_cost DECIMAL(28, 10),
  overhead_cost DECIMAL(28, 10),
  pl_item_cost DECIMAL(28, 10),
  tl_item_cost DECIMAL(28, 10),
  item_cost DECIMAL(28, 10),
  unburdened_cost DECIMAL(28, 10),
  burden_cost DECIMAL(28, 10),
  attribute_category STRING,
  attribute1 STRING,
  attribute2 STRING,
  attribute3 STRING,
  attribute4 STRING,
  attribute5 STRING,
  attribute6 STRING,
  attribute7 STRING,
  attribute8 STRING,
  attribute9 STRING,
  attribute10 STRING,
  attribute11 STRING,
  attribute12 STRING,
  attribute13 STRING,
  attribute14 STRING,
  attribute15 STRING,
  request_id DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  rollup_id DECIMAL(15, 0),
  assignment_set_id DECIMAL(15, 0),
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
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
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
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
FROM bronze_bec_ods_stg.CST_ITEM_COSTS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'cst_item_costs';