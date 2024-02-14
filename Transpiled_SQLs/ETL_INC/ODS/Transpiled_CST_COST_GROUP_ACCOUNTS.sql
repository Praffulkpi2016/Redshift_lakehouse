/* Delete Records */
DELETE FROM silver_bec_ods.CST_COST_GROUP_ACCOUNTS
WHERE
  (COST_GROUP_ID, ORGANIZATION_ID) IN (
    SELECT
      stg.COST_GROUP_ID,
      stg.ORGANIZATION_ID
    FROM silver_bec_ods.CST_COST_GROUP_ACCOUNTS AS ods, bronze_bec_ods_stg.CST_COST_GROUP_ACCOUNTS AS stg
    WHERE
      ods.COST_GROUP_ID = stg.COST_GROUP_ID
      AND ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.CST_COST_GROUP_ACCOUNTS (
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
  kca_operation,
  IS_DELETED_FLG,
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.CST_COST_GROUP_ACCOUNTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COST_GROUP_ID, ORGANIZATION_ID, kca_seq_id) IN (
      SELECT
        COST_GROUP_ID,
        ORGANIZATION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.CST_COST_GROUP_ACCOUNTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COST_GROUP_ID,
        ORGANIZATION_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.CST_COST_GROUP_ACCOUNTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.CST_COST_GROUP_ACCOUNTS SET IS_DELETED_FLG = 'Y'
WHERE
  (COST_GROUP_ID, ORGANIZATION_ID) IN (
    SELECT
      COST_GROUP_ID,
      ORGANIZATION_ID
    FROM bec_raw_dl_ext.CST_COST_GROUP_ACCOUNTS
    WHERE
      (COST_GROUP_ID, ORGANIZATION_ID, KCA_SEQ_ID) IN (
        SELECT
          COST_GROUP_ID,
          ORGANIZATION_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.CST_COST_GROUP_ACCOUNTS
        GROUP BY
          COST_GROUP_ID,
          ORGANIZATION_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'cst_cost_group_accounts';