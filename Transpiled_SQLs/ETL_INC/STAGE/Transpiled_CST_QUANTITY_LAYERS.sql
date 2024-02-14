TRUNCATE table bronze_bec_ods_stg.CST_QUANTITY_LAYERS;
INSERT INTO bronze_bec_ods_stg.CST_QUANTITY_LAYERS (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.CST_QUANTITY_LAYERS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(LAYER_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(LAYER_ID, 0) AS LAYER_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.CST_QUANTITY_LAYERS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(LAYER_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'cst_quantity_layers'
    )
);