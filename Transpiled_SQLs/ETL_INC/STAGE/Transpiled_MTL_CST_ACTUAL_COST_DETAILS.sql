TRUNCATE table
	table bronze_bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS;
INSERT INTO bronze_bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS (
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
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
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
    KCA_OPERATION, /* ,	kca_seq_date */
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_CST_ACTUAL_COST_DETAILS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (LAYER_ID, TRANSACTION_ID, ORGANIZATION_ID, COST_ELEMENT_ID, LEVEL_TYPE, TRANSACTION_ACTION_ID, kca_seq_id) IN (
      SELECT
        LAYER_ID,
        TRANSACTION_ID,
        ORGANIZATION_ID,
        COST_ELEMENT_ID,
        LEVEL_TYPE,
        TRANSACTION_ACTION_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MTL_CST_ACTUAL_COST_DETAILS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        LAYER_ID,
        TRANSACTION_ID,
        ORGANIZATION_ID,
        COST_ELEMENT_ID,
        LEVEL_TYPE,
        TRANSACTION_ACTION_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'mtl_cst_actual_cost_details'
    )
);