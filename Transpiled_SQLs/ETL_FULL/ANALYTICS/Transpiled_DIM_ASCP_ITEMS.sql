DROP table IF EXISTS gold_bec_dwh.DIM_ASCP_ITEMS;
CREATE TABLE gold_bec_dwh.dim_ascp_items AS
(
  SELECT
    plan_id,
    sr_instance_id,
    inventory_item_id,
    item_segments,
    organization_id,
    standard_cost,
    planning_make_buy_code,
    planning_make_buy_code_text,
    description,
    buyer_name,
    planner_code,
    mrp_planning_code_text,
    preprocessing_lead_time,
    postprocessing_lead_time,
    processing_lead_time,
    fixed_lead_time,
    variable_lead_time,
    selling_price,
    margin,
    rounding_control_type,
    repetitive_type,
    carrying_cost,
    wip_supply_type_text,
    abc_class,
    abc_class_name,
    fixed_days_supply,
    fixed_order_quantity,
    fixed_lot_multiplier,
    minimum_order_quantity,
    maximum_order_quantity,
    shrinkage_rate,
    planning_exception_set,
    base_item_id,
    planning_time_fence_date,
    planning_time_fence_days,
    uom_code,
    inventory_use_up_date,
    end_assembly_pegging,
    full_pegging,
    full_pegging_text,
    safety_stock_days,
    safety_stock_percent,
    fixed_safety_stock_qty,
    atp_flag,
    atp_components_flag,
    category_set_id,
    category,
    category_desc,
    'N' AS is_deleted_flg,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) AS source_app_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(plan_id, 0) || '-' || COALESCE(sr_instance_id, 0) || '-' || COALESCE(category_set_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      msi.plan_id,
      msi.sr_instance_id,
      msi.inventory_item_id,
      msi.item_name AS item_segments,
      msi.organization_id,
      msi.standard_cost,
      msi.planning_make_buy_code,
      (
        SELECT
          meaning
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          lookup_type = 'MTL_PLANNING_MAKE_BUY'
          AND lookup_code = msi.planning_make_buy_code
          AND language = 'US'
      ) AS planning_make_buy_code_text,
      msi.description,
      msi.buyer_name,
      msi.planner_code,
      (
        SELECT
          meaning
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          lookup_type = 'MRP_PLANNING_CODE'
          AND lookup_code = msi.mrp_planning_code
          AND language = 'US'
      ) AS mrp_planning_code_text,
      msi.preprocessing_lead_time,
      msi.postprocessing_lead_time,
      msi.full_lead_time AS processing_lead_time,
      msi.fixed_lead_time,
      msi.variable_lead_time,
      msi.list_price AS selling_price,
      COALESCE(msi.list_price, 0) - COALESCE(msi.standard_cost, 0) AS margin,
      msi.rounding_control_type,
      msi.repetitive_type,
      msi.carrying_cost,
      (
        SELECT
          meaning
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          lookup_type = 'WIP_SUPPLY' AND lookup_code = msi.wip_supply_type AND language = 'US'
      ) AS wip_supply_type_text,
      msi.abc_class,
      msi.abc_class_name,
      msi.fixed_days_supply,
      msi.fixed_order_quantity,
      msi.fixed_lot_multiplier,
      msi.minimum_order_quantity,
      msi.maximum_order_quantity,
      msi.shrinkage_rate,
      msi.planning_exception_set,
      msi.base_item_id,
      msi.planning_time_fence_date,
      msi.planning_time_fence_days,
      msi.uom_code,
      msi.inventory_use_up_date,
      msi.end_assembly_pegging_flag AS end_assembly_pegging,
      msi.full_pegging,
      (
        SELECT
          meaning
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          lookup_type = 'MRP_HARD_PEGGING_LEVEL'
          AND lookup_code = msi.full_pegging
          AND language = 'US'
      ) AS full_pegging_text,
      msi.safety_stock_bucket_days AS safety_stock_days,
      msi.safety_stock_percent,
      msi.fixed_safety_stock_qty AS fixed_safety_stock_qty,
      (
        SELECT
          meaning
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          lookup_type = 'MSC_ATP_COMPONENTS_FLAG'
          AND lookup_code = CASE
            WHEN msi.atp_flag = 'N'
            THEN 1
            WHEN msi.atp_flag = 'Y'
            THEN 2
            WHEN msi.atp_flag = 'R'
            THEN 3
            WHEN msi.atp_flag = 'C'
            THEN 4
            ELSE 4
          END
          AND language = 'US'
      ) AS atp_flag,
      (
        SELECT
          meaning
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          lookup_type = 'MSC_ATP_COMPONENTS_FLAG'
          AND lookup_code = CASE
            WHEN msi.atp_components_flag = 'N'
            THEN 1
            WHEN msi.atp_components_flag = 'Y'
            THEN 2
            WHEN msi.atp_components_flag = 'R'
            THEN 3
            WHEN msi.atp_components_flag = 'C'
            THEN 4
            ELSE 4
          END
          AND language = 'US'
      ) AS atp_components_flag,
      mic.category_set_id AS category_set_id,
      mic.category_name AS category,
      mic.description AS category_desc
    FROM silver_bec_ods.msc_system_items AS msi, silver_bec_ods.msc_plans AS plans, silver_bec_ods.msc_item_categories AS mic
    WHERE
      1 = 1
      AND msi.plan_id = plans.plan_id
      AND msi.sr_instance_id = plans.sr_instance_id
      AND msi.sr_instance_id = mic.sr_instance_id
      AND msi.inventory_item_id = mic.inventory_item_id
      AND msi.organization_id = mic.organization_id
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_ascp_items' AND batch_name = 'ascp';