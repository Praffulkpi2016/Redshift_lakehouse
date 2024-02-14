TRUNCATE table bronze_bec_ods_stg.CST_STANDARD_COSTS;
INSERT INTO bronze_bec_ods_stg.CST_STANDARD_COSTS (
  cost_update_id,
  inventory_item_id,
  organization_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  standard_cost_revision_date,
  standard_cost,
  inventory_adjustment_quantity,
  inventory_adjustment_value,
  intransit_adjustment_quantity,
  intransit_adjustment_value,
  wip_adjustment_quantity,
  wip_adjustment_value,
  last_cost_update_id,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    cost_update_id,
    inventory_item_id,
    organization_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    standard_cost_revision_date,
    standard_cost,
    inventory_adjustment_quantity,
    inventory_adjustment_value,
    intransit_adjustment_quantity,
    intransit_adjustment_value,
    wip_adjustment_quantity,
    wip_adjustment_value,
    last_cost_update_id,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.CST_STANDARD_COSTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COST_UPDATE_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, kca_seq_id) IN (
      SELECT
        COST_UPDATE_ID,
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.CST_STANDARD_COSTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COST_UPDATE_ID,
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'cst_standard_costs'
    )
);