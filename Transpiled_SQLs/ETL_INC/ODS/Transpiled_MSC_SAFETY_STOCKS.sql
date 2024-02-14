/* Delete Records */
DELETE FROM silver_bec_ods.MSC_SAFETY_STOCKS
WHERE
  (PLAN_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, PERIOD_START_DATE) IN (
    SELECT
      stg.PLAN_ID,
      stg.INVENTORY_ITEM_ID,
      stg.ORGANIZATION_ID,
      stg.PERIOD_START_DATE
    FROM silver_bec_ods.MSC_SAFETY_STOCKS AS ods, bronze_bec_ods_stg.MSC_SAFETY_STOCKS AS stg
    WHERE
      ods.PLAN_ID = stg.PLAN_ID
      AND ods.INVENTORY_ITEM_ID = stg.INVENTORY_ITEM_ID
      AND ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
      AND ods.PERIOD_START_DATE = stg.PERIOD_START_DATE
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MSC_SAFETY_STOCKS (
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
  kca_operation,
  is_deleted_flg,
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MSC_SAFETY_STOCKS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (PLAN_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, PERIOD_START_DATE, kca_seq_id) IN (
      SELECT
        PLAN_ID,
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        PERIOD_START_DATE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MSC_SAFETY_STOCKS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        PLAN_ID,
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        PERIOD_START_DATE
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MSC_SAFETY_STOCKS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MSC_SAFETY_STOCKS SET IS_DELETED_FLG = 'Y'
WHERE
  (PLAN_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, PERIOD_START_DATE) IN (
    SELECT
      PLAN_ID,
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      PERIOD_START_DATE
    FROM bec_raw_dl_ext.MSC_SAFETY_STOCKS
    WHERE
      (PLAN_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, PERIOD_START_DATE, KCA_SEQ_ID) IN (
        SELECT
          PLAN_ID,
          INVENTORY_ITEM_ID,
          ORGANIZATION_ID,
          PERIOD_START_DATE,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MSC_SAFETY_STOCKS
        GROUP BY
          PLAN_ID,
          INVENTORY_ITEM_ID,
          ORGANIZATION_ID,
          PERIOD_START_DATE
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_safety_stocks';