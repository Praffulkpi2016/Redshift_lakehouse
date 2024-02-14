DROP table IF EXISTS gold_bec_dwh_rpt.FACT_ASCP_HORIZONTAL_PLAN_RT;
CREATE TABLE gold_bec_dwh_rpt.FACT_ASCP_HORIZONTAL_PLAN_RT AS
WITH items(plan_id, organization_id, inventory_item_id, using_assembly_id, level, root_assembly) /* ,path */ AS (
  SELECT DISTINCT
    bom1.plan_id,
    bom1.organization_id,
    bom1.assembly_item_id AS inventory_item_id,
    bom1.assembly_item_id AS using_assembly_id,
    0 AS level,
    bom1.assembly_item_id AS root_assembly
  FROM silver_bec_ods.msc_boms AS bom1, silver_bec_ods.msc_system_items AS msi, gold_bec_dwh.fact_ascp_hp_items_stg AS fahp2
  WHERE
    alternate_bom_designator IS NULL
    AND msi.inventory_item_id = bom1.assembly_item_id
    AND msi.planning_make_buy_code = 1
    AND msi.plan_id = bom1.plan_id
    AND msi.organization_id = bom1.organization_id
    AND fahp2.plan_id = bom1.plan_id
    AND fahp2.organization_id = bom1.organization_id
    AND fahp2.sr_instance_id = bom1.sr_instance_id
    AND fahp2.inventory_item_id = bom1.assembly_item_id
  UNION ALL
  SELECT
    mbc.plan_id,
    mbc.organization_id,
    mbc.inventory_item_id,
    mbc.using_assembly_id,
    level + 1 AS level,
    items.root_assembly
  FROM silver_bec_ods.msc_bom_components AS mbc, gold_bec_dwh.fact_ascp_hp_items_stg AS fahp3, items
  WHERE
    CURRENT_TIMESTAMP() BETWEEN mbc.effectivity_date AND COALESCE(mbc.disable_date, CURRENT_TIMESTAMP())
    AND mbc.effectivity_date <= CURRENT_TIMESTAMP()
    AND fahp3.plan_id = mbc.plan_id
    AND fahp3.organization_id = mbc.organization_id
    AND fahp3.sr_instance_id = mbc.sr_instance_id
    AND fahp3.inventory_item_id = mbc.inventory_item_id
    AND mbc.plan_id = items.plan_id
    AND mbc.organization_id = items.organization_id
    AND mbc.USING_ASSEMBLY_ID = items.inventory_item_id
)
SELECT
  `dim_ascp_items`.`abc_class` AS `abc_class`,
  `dim_ascp_items`.`abc_class_name` AS `abc_class_name`,
  `dim_ascp_items`.`atp_components_flag` AS `atp_components_flag`,
  `dim_ascp_items`.`atp_flag` AS `atp_flag`,
  `dim_ascp_items`.`base_item_id` AS `base_item_id`,
  `dim_ascp_items`.`buyer_name` AS `buyer_name`,
  `dim_ascp_items`.`carrying_cost` AS `carrying_cost`,
  `dim_ascp_items`.`category` AS `category`,
  `dim_ascp_items`.`category_desc` AS `category_desc`,
  `dim_ascp_items`.`category_set_id` AS `category_set_id (dim_ascp_items)`,
  `fact_ascp_horizontal_plan`.`category_set_id` AS `category_set_id`,
  `dim_ascp_plans`.`compile_designator` AS `compile_designator`,
  `dim_ascp_plans`.`curr_cutoff_date` AS `curr_cutoff_date`,
  `dim_ascp_plans`.`curr_plan_type` AS `curr_plan_type`,
  `dim_ascp_plans`.`curr_start_date` AS `curr_start_date`,
  `dim_ascp_plans`.`cutoff_date` AS `cutoff_date`,
  `dim_ascp_plans`.`daily_cutoff_bucket` AS `daily_cutoff_bucket`,
  `dim_ascp_plans`.`data_completion_date` AS `data_completion_date`,
  `dim_ascp_plans`.`data_start_date` AS `data_start_date`,
  `dim_ascp_items`.`description` AS `description (dim_ascp_items)`,
  `dim_ascp_plans`.`description` AS `description`,
  `dim_ascp_items`.`dw_insert_date` AS `dw_insert_date (dim_ascp_items)`,
  `dim_ascp_organizations`.`dw_insert_date` AS `dw_insert_date (dim_ascp_organizations)`,
  `dim_ascp_plans`.`dw_insert_date` AS `dw_insert_date (dim_ascp_plans)`,
  `fact_ascp_horizontal_plan`.`dw_insert_date` AS `dw_insert_date`,
  `dim_ascp_items`.`dw_load_id` AS `dw_load_id (dim_ascp_items)`,
  `dim_ascp_organizations`.`dw_load_id` AS `dw_load_id (dim_ascp_organizations)`,
  `dim_ascp_plans`.`dw_load_id` AS `dw_load_id (dim_ascp_plans)`,
  CAST(`fact_ascp_horizontal_plan`.`dw_load_id` AS STRING) AS `dw_load_id`,
  `dim_ascp_items`.`dw_update_date` AS `dw_update_date (dim_ascp_items)`,
  `dim_ascp_organizations`.`dw_update_date` AS `dw_update_date (dim_ascp_organizations)`,
  `dim_ascp_plans`.`dw_update_date` AS `dw_update_date (dim_ascp_plans)`,
  `fact_ascp_horizontal_plan`.`dw_update_date` AS `dw_update_date`,
  `dim_ascp_items`.`end_assembly_pegging` AS `end_assembly_pegging`,
  `fact_ascp_horizontal_plan`.`fill_kill_flag` AS `fill_kill_flag`,
  `dim_ascp_items`.`fixed_days_supply` AS `fixed_days_supply`,
  `dim_ascp_items`.`fixed_lead_time` AS `fixed_lead_time`,
  `dim_ascp_items`.`fixed_lot_multiplier` AS `fixed_lot_multiplier`,
  `dim_ascp_items`.`fixed_order_quantity` AS `fixed_order_quantity`,
  `dim_ascp_items`.`fixed_safety_stock_qty` AS `fixed_safety_stock_qty`,
  `dim_ascp_items`.`full_pegging` AS `full_pegging`,
  `dim_ascp_items`.`full_pegging_text` AS `full_pegging_text`,
  `dim_ascp_items`.`inventory_item_id` AS `inventory_item_id (dim_ascp_items)`,
  `fact_ascp_horizontal_plan`.`inventory_item_id` AS `inventory_item_id`,
  `fact_ascp_horizontal_plan`.`inventory_item_id_key` AS `inventory_item_id_key`,
  `dim_ascp_items`.`inventory_use_up_date` AS `inventory_use_up_date`,
  `dim_ascp_items`.`is_deleted_flg` AS `is_deleted_flg (dim_ascp_items)`,
  `dim_ascp_organizations`.`is_deleted_flg` AS `is_deleted_flg (dim_ascp_organizations)`,
  `dim_ascp_plans`.`is_deleted_flg` AS `is_deleted_flg (dim_ascp_plans)`,
  `fact_ascp_horizontal_plan`.`is_deleted_flg` AS `is_deleted_flg`,
  `dim_ascp_items`.`item_segments` AS `item_segments`,
  `dim_ascp_items`.`margin` AS `margin`,
  `dim_ascp_organizations`.`master_organization` AS `master_organization`,
  `dim_ascp_items`.`maximum_order_quantity` AS `maximum_order_quantity`,
  `dim_ascp_items`.`minimum_order_quantity` AS `minimum_order_quantity`,
  `dim_ascp_items`.`mrp_planning_code_text` AS `mrp_planning_code_text`,
  `fact_ascp_horizontal_plan`.`new_due_date` AS `new_due_date`,
  `dim_ascp_organizations`.`operating_unit` AS `operating_unit`,
  `fact_ascp_horizontal_plan`.`order_type_entity` AS `order_type_entity`,
  `dim_ascp_organizations`.`organization_code` AS `organization_code`,
  `dim_ascp_items`.`organization_id` AS `organization_id (dim_ascp_items)`,
  `dim_ascp_organizations`.`organization_id` AS `organization_id (dim_ascp_organizations)`,
  `fact_ascp_horizontal_plan`.`organization_id` AS `organization_id`,
  `fact_ascp_horizontal_plan`.`organization_id_key` AS `organization_id_key`,
  `dim_ascp_organizations`.`partner_id` AS `partner_id`,
  `dim_ascp_organizations`.`partner_name` AS `partner_name`,
  `dim_ascp_items`.`plan_id` AS `plan_id (dim_ascp_items)`,
  `dim_ascp_plans`.`plan_id` AS `plan_id (dim_ascp_plans)`,
  `fact_ascp_horizontal_plan`.`plan_id` AS `plan_id`,
  `fact_ascp_horizontal_plan`.`plan_id_key` AS `plan_id_key`,
  `dim_ascp_items`.`planner_code` AS `planner_code`,
  `dim_ascp_items`.`planning_exception_set` AS `planning_exception_set`,
  `dim_ascp_items`.`planning_make_buy_code` AS `planning_make_buy_code`,
  `dim_ascp_items`.`planning_make_buy_code_text` AS `planning_make_buy_code_text`,
  `dim_ascp_items`.`planning_time_fence_date` AS `planning_time_fence_date`,
  `dim_ascp_items`.`planning_time_fence_days` AS `planning_time_fence_days`,
  `dim_ascp_items`.`postprocessing_lead_time` AS `postprocessing_lead_time`,
  `dim_ascp_items`.`preprocessing_lead_time` AS `preprocessing_lead_time`,
  `dim_ascp_items`.`processing_lead_time` AS `processing_lead_time`,
  `fact_ascp_horizontal_plan`.`quantity_rate` AS `quantity_rate`,
  `dim_ascp_items`.`repetitive_type` AS `repetitive_type`,
  `dim_ascp_items`.`rounding_control_type` AS `rounding_control_type`,
  `dim_ascp_items`.`safety_stock_days` AS `safety_stock_days`,
  `dim_ascp_items`.`safety_stock_percent` AS `safety_stock_percent`,
  `dim_ascp_items`.`selling_price` AS `selling_price`,
  `dim_ascp_items`.`shrinkage_rate` AS `shrinkage_rate`,
  `fact_ascp_horizontal_plan`.`so_line_split` AS `so_line_split`,
  `dim_ascp_items`.`source_app_id` AS `source_app_id (dim_ascp_items)`,
  `dim_ascp_organizations`.`source_app_id` AS `source_app_id (dim_ascp_organizations)`,
  `dim_ascp_plans`.`source_app_id` AS `source_app_id (dim_ascp_plans)`,
  `fact_ascp_horizontal_plan`.`source_app_id` AS `source_app_id`,
  `dim_ascp_items`.`sr_instance_id` AS `sr_instance_id (dim_ascp_items)`,
  `dim_ascp_organizations`.`sr_instance_id` AS `sr_instance_id (dim_ascp_organizations)`,
  `dim_ascp_plans`.`sr_instance_id` AS `sr_instance_id (dim_ascp_plans)`,
  `fact_ascp_horizontal_plan`.`sr_instance_id` AS `sr_instance_id`,
  `dim_ascp_items`.`standard_cost` AS `standard_cost`,
  `dim_ascp_items`.`uom_code` AS `uom_code`,
  `dim_ascp_items`.`variable_lead_time` AS `variable_lead_time`,
  `dim_ascp_plans`.`weekly_cutoff_bucket` AS `weekly_cutoff_bucket`,
  `dim_ascp_items`.`wip_supply_type_text` AS `wip_supply_type_text`,
  i.level,
  `dim_ascp_items_2`.`item_segments` AS `root_item_segments`
FROM `bec_dwh`.`fact_ascp_horizontal_plan` AS `fact_ascp_horizontal_plan`
LEFT JOIN `bec_dwh`.`dim_ascp_plans` AS `dim_ascp_plans`
  ON (
    (
      `fact_ascp_horizontal_plan`.`plan_id` = `dim_ascp_plans`.`plan_id`
    )
    AND (
      `fact_ascp_horizontal_plan`.`sr_instance_id` = `dim_ascp_plans`.`sr_instance_id`
    )
  )
LEFT JOIN `bec_dwh`.`dim_ascp_organizations` AS `dim_ascp_organizations`
  ON (
    (
      `fact_ascp_horizontal_plan`.`organization_id` = `dim_ascp_organizations`.`organization_id`
    )
    AND (
      `fact_ascp_horizontal_plan`.`sr_instance_id` = `dim_ascp_organizations`.`sr_instance_id`
    )
    AND `dim_ascp_organizations`.is_deleted_flg = 'N'
  )
LEFT JOIN `bec_dwh`.`dim_ascp_items` AS `dim_ascp_items`
  ON (
    (
      `fact_ascp_horizontal_plan`.`inventory_item_id` = `dim_ascp_items`.`inventory_item_id`
    )
    AND (
      `fact_ascp_horizontal_plan`.`organization_id` = `dim_ascp_items`.`organization_id`
    )
    AND (
      `fact_ascp_horizontal_plan`.`plan_id` = `dim_ascp_items`.`plan_id`
    )
    AND (
      `fact_ascp_horizontal_plan`.`category_set_id` = `dim_ascp_items`.`category_set_id`
    )
  )
INNER JOIN items AS i
  ON fact_ascp_horizontal_plan.plan_id = i.plan_id
  AND fact_ascp_horizontal_plan.organization_id = i.organization_id
  AND fact_ascp_horizontal_plan.inventory_item_id = i.inventory_item_id
LEFT JOIN `bec_dwh`.`dim_ascp_items` AS `dim_ascp_items_2`
  ON (
    (
      i.`root_assembly` = `dim_ascp_items_2`.`inventory_item_id`
    )
    AND (
      i.`organization_id` = `dim_ascp_items_2`.`organization_id`
    )
    AND (
      i.`plan_id` = `dim_ascp_items_2`.`plan_id`
    )
    AND (
      `dim_ascp_items`.`category_set_id` = 9
    )
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ascp_horizontal_plan_rt' AND batch_name = 'ascp';