TRUNCATE table bronze_bec_ods_stg.CST_COST_UPDATES;
INSERT INTO bronze_bec_ods_stg.CST_COST_UPDATES (
  cost_update_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  status,
  organization_id,
  cost_type_id,
  update_date,
  description,
  range_option,
  update_resource_ovhd_flag,
  update_activity_flag,
  snapshot_saved_flag,
  inv_adjustment_account,
  single_item,
  item_range_low,
  item_range_high,
  category_id,
  category_set_id,
  inventory_adjustment_value,
  intransit_adjustment_value,
  wip_adjustment_value,
  scrap_adjustment_value,
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
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    status,
    organization_id,
    cost_type_id,
    update_date,
    description,
    range_option,
    update_resource_ovhd_flag,
    update_activity_flag,
    snapshot_saved_flag,
    inv_adjustment_account,
    single_item,
    item_range_low,
    item_range_high,
    category_id,
    category_set_id,
    inventory_adjustment_value,
    intransit_adjustment_value,
    wip_adjustment_value,
    scrap_adjustment_value,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.CST_COST_UPDATES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(COST_UPDATE_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(COST_UPDATE_ID, 0) AS COST_UPDATE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.CST_COST_UPDATES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(COST_UPDATE_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'cst_cost_updates'
    )
);