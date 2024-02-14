TRUNCATE table silver_bec_ods.CST_ITEM_COST_DETAILS;
INSERT INTO silver_bec_ods.CST_ITEM_COST_DETAILS (
  inventory_item_id,
  organization_id,
  cost_type_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  operation_sequence_id,
  operation_seq_num,
  department_id,
  level_type,
  activity_id,
  resource_seq_num,
  resource_id,
  resource_rate,
  item_units,
  activity_units,
  usage_rate_or_amount,
  basis_type,
  basis_resource_id,
  basis_factor,
  net_yield_or_shrinkage_factor,
  item_cost,
  cost_element_id,
  rollup_source_type,
  activity_context,
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
  yielded_cost,
  source_organization_id,
  vendor_id,
  allocation_percent,
  vendor_site_id,
  ship_method,
  basis_res_basis_type,
  bom_component_cost_flag,
  cmi_cp_plan_line_id,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT DISTINCT
  inventory_item_id,
  organization_id,
  cost_type_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  operation_sequence_id,
  operation_seq_num,
  department_id,
  level_type,
  activity_id,
  resource_seq_num,
  resource_id,
  resource_rate,
  item_units,
  activity_units,
  usage_rate_or_amount,
  basis_type,
  basis_resource_id,
  basis_factor,
  net_yield_or_shrinkage_factor,
  item_cost,
  cost_element_id,
  rollup_source_type,
  activity_context,
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
  yielded_cost,
  source_organization_id,
  vendor_id,
  allocation_percent,
  vendor_site_id,
  ship_method,
  basis_res_basis_type,
  bom_component_cost_flag,
  cmi_cp_plan_line_id,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.CST_ITEM_COST_DETAILS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'cst_item_cost_details';