TRUNCATE table
	table bronze_bec_ods_stg.CST_COST_GROUP_ACCOUNTS;
INSERT INTO bronze_bec_ods_stg.CST_COST_GROUP_ACCOUNTS (
  cost_group_id,
  organization_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  material_account,
  material_overhead_account,
  resource_account,
  overhead_account,
  outside_processing_account,
  average_cost_var_account,
  encumbrance_account,
  payback_mat_var_account,
  payback_res_var_account,
  payback_osp_var_account,
  payback_moh_var_account,
  payback_ovh_var_account,
  expense_account,
  purchase_price_var_account,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    cost_group_id,
    organization_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    material_account,
    material_overhead_account,
    resource_account,
    overhead_account,
    outside_processing_account,
    average_cost_var_account,
    encumbrance_account,
    payback_mat_var_account,
    payback_res_var_account,
    payback_osp_var_account,
    payback_moh_var_account,
    payback_ovh_var_account,
    expense_account,
    purchase_price_var_account,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.CST_COST_GROUP_ACCOUNTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COST_GROUP_ID, ORGANIZATION_ID, kca_seq_id) IN (
      SELECT
        COST_GROUP_ID,
        ORGANIZATION_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.CST_COST_GROUP_ACCOUNTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COST_GROUP_ID,
        ORGANIZATION_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'cst_cost_group_accounts'
    )
);