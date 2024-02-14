DROP table IF EXISTS gold_bec_dwh.FACT_ASCP_ASSEMBLY_COMPS;
CREATE TABLE gold_bec_dwh.FACT_ASCP_ASSEMBLY_COMPS AS
(
  WITH assembly_items AS (
    SELECT DISTINCT
      msi.inventory_item_Id,
      msi.organization_id
    FROM silver_bec_ods.mrp_forecast_dates AS mfd, silver_bec_ods.msc_system_items AS msi
    WHERE
      1 = 1
      AND forecast_date > CURRENT_TIMESTAMP()
      AND current_forecast_quantity > 0
      AND mfd.organization_id = msi.organization_id
      AND forecast_designator LIKE '%.%'
      AND mfd.inventory_item_Id = msi.sr_inventory_item_id
      AND msi.plan_id = 40029
      AND msi.planning_make_buy_code = 1
  ) /* and msi.inventory_item_Id = 2210003 */, mov_forecast_qty AS (
    SELECT
      using_assembly_item_id,
      inventory_item_id,
      organization_id,
      FLOOR(new_due_date) AS new_due_date,
      SUM(quantity_rate) AS forecast_qty
    FROM silver_bec_ods.msc_orders_v
    WHERE
      order_type_text = 'Forecast'
      AND plan_id = 40029
      AND category_id = 3411
      AND using_assembly_item_id = inventory_item_id
      AND planning_make_buy_code = 1
      AND quantity_rate < 0
    GROUP BY
      using_assembly_item_id,
      inventory_item_id,
      organization_id,
      FLOOR(new_due_date)
  )
  SELECT
    msia.item_name AS assembly_item,
    msia.description AS assembly_item_desc,
    msia.sr_inventory_item_id AS assembly_sr_mtl_item_id,
    i.using_assembly_item_id AS Assembly_item_id,
    msic.item_name AS component_item,
    msic.description AS component_item_desc,
    msic.sr_inventory_item_id AS component_sr_mtl_item_id,
    msic.buyer_name AS component_buyer_name,
    msic.planner_code AS component_planner_code,
    i.inventory_item_id AS component_item_id,
    i.organization_id,
    i.quantity_per_assembly AS comp_qty_per_assembly
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY organization_id, using_assembly_item_id, inventory_item_id ORDER BY organization_id NULLS LAST, using_assembly_item_id NULLS LAST, inventory_item_id NULLS LAST, assembly_count DESC NULLS FIRST) AS rownumber
    FROM (
      SELECT
        using_assembly_item_id,
        inventory_item_id,
        organization_id,
        quantity_per_assembly,
        COUNT(quantity_per_assembly) AS assembly_count
      FROM (
        SELECT
          mov.using_assembly_item_id,
          mov.inventory_item_id,
          mov.organization_id,
          FLOOR(mov.new_due_date) AS new_due_date,
          SUM(mov.quantity_rate) AS mov_plan_quantity,
          (
            SELECT
              SUM(fc.forecast_qty)
            FROM mov_forecast_qty AS fc
            WHERE
              mov.using_assembly_item_id = fc.using_assembly_item_id
              AND mov.organization_id = fc.organization_id
              AND FLOOR(mov.new_due_date) = fc.new_due_date
          ) AS total_forecast,
          SUM(mov.quantity_rate) / (
            SELECT
              SUM(fc.forecast_qty)
            FROM mov_forecast_qty AS fc
            WHERE
              mov.using_assembly_item_id = fc.using_assembly_item_id
              AND mov.organization_id = fc.organization_id
              AND FLOOR(mov.new_due_date) = fc.new_due_date
          ) AS quantity_per_assembly
        FROM assembly_items AS items, silver_bec_ods.msc_orders_v AS mov
        WHERE
          mov.order_type_text = 'Planned order demand'
          AND mov.plan_id = 40029
          AND mov.category_id = 3411
          AND mov.using_assembly_item_id <> mov.inventory_item_id
          AND mov.planning_make_buy_code = 2
          AND FLOOR(mov.new_due_date) > FLOOR(CURRENT_TIMESTAMP())
          AND mov.quantity_rate < 0
          AND mov.using_assembly_item_id = items.inventory_item_Id
          AND mov.organization_id = items.organization_id
        GROUP BY
          mov.using_assembly_item_id,
          mov.new_due_date,
          mov.inventory_item_id,
          mov.organization_id
      )
      WHERE
        NOT total_forecast IS NULL
      GROUP BY
        using_assembly_item_id,
        inventory_item_id,
        organization_id,
        quantity_per_assembly
    )
  ) AS i, silver_bec_ods.msc_system_items AS msia, silver_bec_ods.msc_system_items AS msic
  WHERE
    rownumber = 1
    AND msia.inventory_item_id = i.using_assembly_item_id
    AND msia.organization_id = i.organization_id
    AND msia.plan_id = 40029
    AND msic.inventory_item_id = i.inventory_item_id
    AND msic.organization_id = i.organization_id
    AND msic.plan_id = 40029
  ORDER BY
    i.organization_id NULLS LAST,
    i.using_assembly_item_id NULLS LAST,
    i.inventory_item_id NULLS LAST,
    i.assembly_count DESC NULLS FIRST
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ascp_assembly_comps' AND batch_name = 'ascp';