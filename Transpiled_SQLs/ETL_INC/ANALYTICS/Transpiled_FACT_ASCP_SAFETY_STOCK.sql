TRUNCATE table gold_bec_dwh.FACT_ASCP_SAFETY_STOCK;
INSERT INTO gold_bec_dwh.FACT_ASCP_SAFETY_STOCK
(
  SELECT
    mss.organization_id,
    msi.segment1 AS item,
    msi.description,
    msi.primary_uom_code,
    mss.effectivity_date,
    mss.safety_stock_quantity,
    msi.planner_code,
    pov.agent_name AS buyer_name,
    mss.created_by,
    mss.last_updated_by,
    mp.employee_id AS planner_emp_id,
    cis.item_cost AS std_cost,
    (
      cis.item_cost * mss.safety_stock_quantity
    ) AS extended_cost,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || mss.organization_id AS organization_id_key,
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
    ) || '-' || COALESCE(mss.organization_id, 0) || '-' || COALESCE(item, 'NA') || '-' || COALESCE(effectivity_date, '1900-01-01 12:00:00') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date /* item,effectivity_date */
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_safety_stocks
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS mss, (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS msi, (
    SELECT
      *
    FROM silver_bec_ods.mtl_planners
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS mp, (
    SELECT
      *
    FROM silver_bec_ods.po_agents_v
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS pov, (
    SELECT
      *
    FROM silver_bec_ods.cst_item_costs
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS cis
  WHERE
    mss.inventory_item_id = msi.inventory_item_id
    AND mss.organization_id = msi.organization_id
    AND msi.planner_code = mp.PLANNER_CODE()
    AND msi.organization_id = mp.ORGANIZATION_ID()
    AND msi.buyer_id = pov.AGENT_ID()
    AND cis.inventory_item_id = mss.inventory_item_id
    AND cis.organization_id = mss.organization_id
    AND msi.inventory_item_id = cis.inventory_item_id
    AND msi.organization_id = cis.organization_id
    AND cost_type_id = '1'
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ascp_safety_stock' AND batch_name = 'ascp';