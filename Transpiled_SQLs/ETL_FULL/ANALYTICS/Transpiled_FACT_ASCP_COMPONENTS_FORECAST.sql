DROP table IF EXISTS gold_bec_dwh.FACT_ASCP_COMPONENTS_FORECAST;
CREATE TABLE gold_bec_dwh.fact_ascp_components_forecast AS
(
  WITH forecast_data AS (
    SELECT
      faf.organization_id,
      faf.part_number,
      faf.item_description,
      faf.inventory_item_id,
      faf.buyer_name,
      faf.planner_code,
      faf.forecast_date,
      faf.forecast_designator,
      PLANNING_MAKE_BUY_CODE,
      SUM(faf.current_forecast_quantity) AS current_forecast_quantity
    FROM gold_bec_dwh.fact_ascp_forecast AS faf
    WHERE
      faf.current_forecast_quantity > 0
    GROUP BY
      faf.organization_id,
      faf.part_number,
      faf.item_description,
      faf.inventory_item_id,
      faf.buyer_name,
      faf.planner_code,
      faf.forecast_date,
      faf.forecast_designator,
      PLANNING_MAKE_BUY_CODE
  )
  SELECT
    faf.organization_id,
    faf.part_number AS assembly_item,
    faf.item_description AS assembly_item_desc,
    faf.inventory_item_id AS assembly_item_id,
    faf.part_number AS component_item,
    faf.item_description AS component_item_desc,
    faf.inventory_item_id AS component_item_id,
    faf.buyer_name,
    faf.planner_code,
    faf.forecast_date,
    faf.forecast_designator,
    NULL AS component_qty,
    faf.current_forecast_quantity AS forecast_qty,
    faf.current_forecast_quantity AS component_forecast_qty
  FROM forecast_data AS faf
  WHERE
    PLANNING_MAKE_BUY_CODE = 2
  UNION
  SELECT
    faf.organization_id,
    faf.part_number AS assembly_item,
    faf.item_description AS assembly_item_desc,
    faf.inventory_item_id AS assembly_item_id,
    comp.component_item,
    comp.component_item_desc,
    comp.component_sr_mtl_item_id AS component_item_id,
    comp.component_buyer_name AS buyer_name,
    comp.component_planner_code AS planner_code,
    faf.forecast_date,
    faf.forecast_designator,
    comp_qty_per_assembly AS component_qty,
    faf.current_forecast_quantity AS forecast_qty,
    (
      faf.current_forecast_quantity * comp_qty_per_assembly
    ) AS component_forecast_qty
  FROM forecast_data AS faf, gold_bec_dwh.fact_ascp_assembly_comps AS comp
  WHERE
    faf.PLANNING_MAKE_BUY_CODE = 1
    AND faf.inventory_item_id = comp.assembly_sr_mtl_item_id
    AND faf.organization_id = comp.organization_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ascp_components_forecast' AND batch_name = 'ascp';