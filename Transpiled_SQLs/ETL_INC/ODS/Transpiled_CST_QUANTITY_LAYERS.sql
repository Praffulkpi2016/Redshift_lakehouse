/* Delete Records */
DELETE FROM silver_bec_ods.CST_QUANTITY_LAYERS
WHERE
  (
    COALESCE(LAYER_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.LAYER_ID, 0) AS LAYER_ID
    FROM silver_bec_ods.CST_QUANTITY_LAYERS AS ods, bronze_bec_ods_stg.CST_QUANTITY_LAYERS AS stg
    WHERE
      COALESCE(ods.LAYER_ID, 0) = COALESCE(stg.LAYER_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.CST_QUANTITY_LAYERS (
  layer_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  organization_id,
  inventory_item_id,
  layer_quantity,
  create_transaction_id,
  update_transaction_id,
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
  cost_group_id,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    layer_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    organization_id,
    inventory_item_id,
    layer_quantity,
    create_transaction_id,
    update_transaction_id,
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
    cost_group_id,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.CST_QUANTITY_LAYERS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(LAYER_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(LAYER_ID, 0) AS LAYER_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.CST_QUANTITY_LAYERS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(LAYER_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.CST_QUANTITY_LAYERS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.CST_QUANTITY_LAYERS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    LAYER_ID
  ) IN (
    SELECT
      LAYER_ID
    FROM bec_raw_dl_ext.CST_QUANTITY_LAYERS
    WHERE
      (LAYER_ID, KCA_SEQ_ID) IN (
        SELECT
          LAYER_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.CST_QUANTITY_LAYERS
        GROUP BY
          LAYER_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'cst_quantity_layers';