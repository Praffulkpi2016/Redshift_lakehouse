DROP TABLE IF EXISTS silver_bec_ods.MTL_CST_ACTUAL_COST_DETAILS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.MTL_CST_ACTUAL_COST_DETAILS (
  layer_id DECIMAL(15, 0),
  transaction_id DECIMAL(15, 0),
  organization_id DECIMAL(15, 0),
  cost_element_id DECIMAL(15, 0),
  level_type DECIMAL(15, 0),
  transaction_action_id DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  request_id DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  inventory_item_id DECIMAL(15, 0),
  actual_cost DECIMAL(28, 10),
  prior_cost DECIMAL(28, 10),
  new_cost DECIMAL(28, 10),
  insertion_flag STRING,
  variance_amount DECIMAL(28, 10),
  user_entered STRING,
  transaction_costed_date TIMESTAMP,
  payback_variance_amount DECIMAL(28, 10),
  onhand_variance_amount DECIMAL(28, 10),
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.MTL_CST_ACTUAL_COST_DETAILS (
  layer_id,
  transaction_id,
  organization_id,
  cost_element_id,
  level_type,
  transaction_action_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  inventory_item_id,
  actual_cost,
  prior_cost,
  new_cost,
  insertion_flag,
  variance_amount,
  user_entered,
  transaction_costed_date,
  payback_variance_amount,
  onhand_variance_amount,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  layer_id,
  transaction_id,
  organization_id,
  cost_element_id,
  level_type,
  transaction_action_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  inventory_item_id,
  actual_cost,
  prior_cost,
  new_cost,
  insertion_flag,
  variance_amount,
  user_entered,
  transaction_costed_date,
  payback_variance_amount,
  onhand_variance_amount,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_cst_actual_cost_details';