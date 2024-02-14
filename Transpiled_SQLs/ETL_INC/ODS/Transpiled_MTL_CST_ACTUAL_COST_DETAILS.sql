/* Delete Records */
DELETE FROM silver_bec_ods.MTL_CST_ACTUAL_COST_DETAILS
WHERE
  (LAYER_ID, TRANSACTION_ID, ORGANIZATION_ID, COST_ELEMENT_ID, LEVEL_TYPE, TRANSACTION_ACTION_ID) IN (
    SELECT
      stg.LAYER_ID,
      stg.TRANSACTION_ID,
      stg.ORGANIZATION_ID,
      stg.COST_ELEMENT_ID,
      stg.LEVEL_TYPE,
      stg.TRANSACTION_ACTION_ID
    FROM silver_bec_ods.MTL_CST_ACTUAL_COST_DETAILS AS ods, bronze_bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS AS stg
    WHERE
      ods.LAYER_ID = stg.LAYER_ID
      AND ods.TRANSACTION_ID = stg.TRANSACTION_ID
      AND ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
      AND ods.COST_ELEMENT_ID = stg.COST_ELEMENT_ID
      AND ods.LEVEL_TYPE = stg.LEVEL_TYPE
      AND ods.TRANSACTION_ACTION_ID = stg.TRANSACTION_ACTION_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (LAYER_ID, TRANSACTION_ID, ORGANIZATION_ID, COST_ELEMENT_ID, LEVEL_TYPE, TRANSACTION_ACTION_ID, kca_seq_id) IN (
      SELECT
        LAYER_ID,
        TRANSACTION_ID,
        ORGANIZATION_ID,
        COST_ELEMENT_ID,
        LEVEL_TYPE,
        TRANSACTION_ACTION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MTL_CST_ACTUAL_COST_DETAILS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        LAYER_ID,
        TRANSACTION_ID,
        ORGANIZATION_ID,
        COST_ELEMENT_ID,
        LEVEL_TYPE,
        TRANSACTION_ACTION_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_CST_ACTUAL_COST_DETAILS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_CST_ACTUAL_COST_DETAILS SET IS_DELETED_FLG = 'Y'
WHERE
  (LAYER_ID, TRANSACTION_ID, ORGANIZATION_ID, COST_ELEMENT_ID, LEVEL_TYPE, TRANSACTION_ACTION_ID) IN (
    SELECT
      LAYER_ID,
      TRANSACTION_ID,
      ORGANIZATION_ID,
      COST_ELEMENT_ID,
      LEVEL_TYPE,
      TRANSACTION_ACTION_ID
    FROM bec_raw_dl_ext.MTL_CST_ACTUAL_COST_DETAILS
    WHERE
      (LAYER_ID, TRANSACTION_ID, ORGANIZATION_ID, COST_ELEMENT_ID, LEVEL_TYPE, TRANSACTION_ACTION_ID, KCA_SEQ_ID) IN (
        SELECT
          LAYER_ID,
          TRANSACTION_ID,
          ORGANIZATION_ID,
          COST_ELEMENT_ID,
          LEVEL_TYPE,
          TRANSACTION_ACTION_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_CST_ACTUAL_COST_DETAILS
        GROUP BY
          LAYER_ID,
          TRANSACTION_ID,
          ORGANIZATION_ID,
          COST_ELEMENT_ID,
          LEVEL_TYPE,
          TRANSACTION_ACTION_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_cst_actual_cost_details';