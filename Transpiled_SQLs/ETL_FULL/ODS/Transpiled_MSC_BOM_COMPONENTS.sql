DROP TABLE IF EXISTS silver_bec_ods.MSC_BOM_COMPONENTS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.MSC_BOM_COMPONENTS (
  plan_id DECIMAL(15, 0),
  component_sequence_id DECIMAL(15, 0),
  bill_sequence_id DECIMAL(15, 0),
  sr_instance_id DECIMAL(15, 0),
  organization_id DECIMAL(15, 0),
  inventory_item_id DECIMAL(15, 0),
  using_assembly_id DECIMAL(15, 0),
  component_type DECIMAL(28, 10),
  scaling_type DECIMAL(15, 0),
  change_notice STRING,
  revision STRING,
  uom_code STRING,
  usage_quantity DECIMAL(28, 10),
  effectivity_date TIMESTAMP,
  disable_date TIMESTAMP,
  from_unit_number STRING,
  to_unit_number STRING,
  use_up_code DECIMAL(15, 0),
  suggested_effectivity_date TIMESTAMP,
  driving_item_id DECIMAL(15, 0),
  operation_offset_percent DECIMAL(28, 10),
  optional_component DECIMAL(15, 0),
  old_effectivity_date TIMESTAMP,
  wip_supply_type DECIMAL(15, 0),
  planning_factor DECIMAL(15, 0),
  atp_flag DECIMAL(15, 0),
  component_yield_factor DECIMAL(28, 10),
  refresh_number DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  request_id DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
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
  scale_multiple DECIMAL(28, 10),
  scale_rounding_variance DECIMAL(28, 10),
  rounding_direction DECIMAL(15, 0),
  primary_flag DECIMAL(28, 10),
  contribute_to_step_qty DECIMAL(28, 10),
  old_component_sequence_id DECIMAL(15, 0),
  operation_seq_num DECIMAL(15, 0),
  new_plan_id DECIMAL(15, 0),
  new_plan_list STRING,
  applied DECIMAL(28, 10),
  simulation_set_id DECIMAL(15, 0),
  base_model_item_id DECIMAL(15, 0),
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.MSC_BOM_COMPONENTS (
  plan_id,
  component_sequence_id,
  bill_sequence_id,
  sr_instance_id,
  organization_id,
  inventory_item_id,
  using_assembly_id,
  component_type,
  scaling_type,
  change_notice,
  revision,
  uom_code,
  usage_quantity,
  effectivity_date,
  disable_date,
  from_unit_number,
  to_unit_number,
  use_up_code,
  suggested_effectivity_date,
  driving_item_id,
  operation_offset_percent,
  optional_component,
  old_effectivity_date,
  wip_supply_type,
  planning_factor,
  atp_flag,
  component_yield_factor,
  refresh_number,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
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
  scale_multiple,
  scale_rounding_variance,
  rounding_direction,
  primary_flag,
  contribute_to_step_qty,
  old_component_sequence_id,
  operation_seq_num,
  new_plan_id,
  new_plan_list,
  applied,
  simulation_set_id,
  base_model_item_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  plan_id,
  component_sequence_id,
  bill_sequence_id,
  sr_instance_id,
  organization_id,
  inventory_item_id,
  using_assembly_id,
  component_type,
  scaling_type,
  change_notice,
  revision,
  uom_code,
  usage_quantity,
  effectivity_date,
  disable_date,
  from_unit_number,
  to_unit_number,
  use_up_code,
  suggested_effectivity_date,
  driving_item_id,
  operation_offset_percent,
  optional_component,
  old_effectivity_date,
  wip_supply_type,
  planning_factor,
  atp_flag,
  component_yield_factor,
  refresh_number,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
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
  scale_multiple,
  scale_rounding_variance,
  rounding_direction,
  primary_flag,
  contribute_to_step_qty,
  old_component_sequence_id,
  operation_seq_num,
  new_plan_id,
  new_plan_list,
  applied,
  simulation_set_id,
  base_model_item_id,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.MSC_BOM_COMPONENTS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_bom_components';