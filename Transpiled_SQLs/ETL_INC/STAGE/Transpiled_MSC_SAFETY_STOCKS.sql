TRUNCATE table
	table bronze_bec_ods_stg.MSC_SAFETY_STOCKS;
INSERT INTO bronze_bec_ods_stg.MSC_SAFETY_STOCKS (
  plan_id,
  organization_id,
  sr_instance_id,
  inventory_item_id,
  period_start_date,
  safety_stock_quantity,
  updated,
  status,
  refresh_number,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_id,
  program_update_date,
  program_application_id,
  target_safety_stock,
  project_id,
  task_id,
  planning_group,
  user_defined_safety_stocks,
  user_defined_dos,
  target_days_of_supply,
  achieved_days_of_supply,
  unit_number,
  demand_var_ss_percent,
  mfg_ltvar_ss_percent,
  transit_ltvar_ss_percent,
  sup_ltvar_ss_percent,
  total_unpooled_safety_stock,
  item_type_id,
  item_type_value,
  new_plan_id,
  simulation_set_id,
  new_plan_list,
  applied,
  reserved_safety_stock_qty,
  inventory_level,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    plan_id,
    organization_id,
    sr_instance_id,
    inventory_item_id,
    period_start_date,
    safety_stock_quantity,
    updated,
    status,
    refresh_number,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_id,
    program_update_date,
    program_application_id,
    target_safety_stock,
    project_id,
    task_id,
    planning_group,
    user_defined_safety_stocks,
    user_defined_dos,
    target_days_of_supply,
    achieved_days_of_supply,
    unit_number,
    demand_var_ss_percent,
    mfg_ltvar_ss_percent,
    transit_ltvar_ss_percent,
    sup_ltvar_ss_percent,
    total_unpooled_safety_stock,
    item_type_id,
    item_type_value,
    new_plan_id,
    simulation_set_id,
    new_plan_list,
    applied,
    reserved_safety_stock_qty,
    inventory_level,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MSC_SAFETY_STOCKS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (PLAN_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, PERIOD_START_DATE, kca_seq_id) IN (
      SELECT
        PLAN_ID,
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        PERIOD_START_DATE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MSC_SAFETY_STOCKS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        PLAN_ID,
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        PERIOD_START_DATE
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'msc_safety_stocks'
      )
    )
);