DROP table IF EXISTS gold_bec_dwh.FACT_ASCP_PLAN_PARAMS;
CREATE TABLE gold_bec_dwh.FACT_ASCP_PLAN_PARAMS AS
(
  WITH safety_stock AS (
    SELECT
      safety_stock_quantity,
      inventory_item_id,
      organization_id
    FROM silver_bec_ods.MTL_SAFETY_STOCKS
    WHERE
      IS_DELETED_FLG <> 'Y'
      AND (effectivity_date, organization_id, inventory_item_id) IN (
        SELECT
          MAX(effectivity_date) AS effectivity_date,
          organization_id,
          inventory_item_id
        FROM silver_bec_ods.MTL_SAFETY_STOCKS
        WHERE
          1 = 1 AND IS_DELETED_FLG <> 'Y'
        GROUP BY
          organization_id,
          inventory_item_id
      )
  ), onhand_qty AS (
    SELECT
      SUM(transaction_quantity) AS onhand_quantity,
      moq.organization_id AS moq_org_id,
      inventory_item_id AS moq_item_id,
      ms.availability_type
    FROM (
      SELECT
        *
      FROM silver_bec_ods.mtl_onhand_quantities
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS moq, (
      SELECT
        *
      FROM silver_bec_ods.mtl_secondary_inventories
      WHERE
        IS_DELETED_FLG <> 'Y'
    ) AS ms
    WHERE
      moq.subinventory_code = ms.secondary_inventory_name
      AND moq.organization_id = ms.organization_id
      AND ms.availability_type IN (1, 2)
    GROUP BY
      moq.organization_id,
      inventory_item_id,
      ms.availability_type
  ), mtl_planners AS (
    SELECT
      planner_code,
      organization_id,
      employee_id
    FROM silver_bec_ods.mtl_planners
    WHERE
      IS_DELETED_FLG <> 'Y'
      AND (
        disable_date IS NULL OR disable_date > CURRENT_TIMESTAMP()
      )
  )
  SELECT
    msi.segment1 AS item_number,
    msi.description,
    msi.creation_date,
    msi.primary_uom_code,
    msi.primary_unit_of_measure,
    CASE
      WHEN msi.planning_make_buy_code = 1
      THEN 'Make'
      WHEN msi.planning_make_buy_code = 2
      THEN 'Buy'
    END AS make_buy_flag,
    msi.inventory_item_status_code,
    cic.item_cost AS std_unit_cost,
    pob.agent_name AS buyer,
    msi.postprocessing_lead_time,
    msi.preprocessing_lead_time,
    ml2.meaning AS min_max_planned,
    msi.fixed_lead_time,
    msi.cum_manufacturing_lead_time,
    msi.cumulative_total_lead_time,
    ml3.meaning AS mrp_planned,
    msi.planner_code,
    msi.full_lead_time,
    msi.min_minmax_quantity,
    msi.max_minmax_quantity,
    msi.minimum_order_quantity,
    msi.maximum_order_quantity,
    msi.fixed_order_quantity,
    msi.fixed_days_supply,
    msi.planning_time_fence_code,
    msi.planning_time_fence_days,
    ml1.meaning AS wip_supply_type,
    1 AS category_set_id,
    mss.safety_stock_quantity,
    msi.organization_id,
    msi.inventory_item_id,
    CASE
      WHEN msi.lot_control_code = 1
      THEN 'N'
      WHEN msi.lot_control_code = 2
      THEN 'Y'
    END AS lot_control,
    CASE WHEN msi.serial_number_control_code = 1 THEN 'N' ELSE 'Y' END AS serial_control,
    msi.comms_nl_trackable_flag AS install_base_trackable,
    CASE
      WHEN msi.serv_req_enabled_code = 'E'
      THEN 'Enabled'
      WHEN msi.serv_req_enabled_code = 'D'
      THEN 'Disabled'
      WHEN msi.serv_req_enabled_code = 'I'
      THEN 'Inactive'
      ELSE NULL
    END AS service_request_enabled,
    msi.shrinkage_rate,
    msi.inspection_required_flag,
    msi.release_time_fence_code,
    msi.release_time_fence_days,
    moh.ONHAND_QUANTITY,
    moh_nn.ONHAND_QUANTITY AS NN_onhand_quantity,
    msi.attribute5 AS program_name,
    CASE WHEN mp.planner_code IS NULL THEN 'Inactive' ELSE 'Active' END AS planner_status, /*    mp.planner_code, */
    mp.employee_id AS planner_emp_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || msi.organization_id AS organization_id_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || msi.inventory_item_id AS inventory_item_id_key,
    'N' AS is_deleted_flg, /* audit COLUMNS */
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
    ) || '-' || COALESCE(msi.organization_id, 0) || '-' || COALESCE(msi.inventory_item_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_system_items_b
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS msi, (
    SELECT
      *
    FROM silver_bec_ods.cst_item_costs
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS cic, (
    SELECT
      *
    FROM silver_bec_ods.po_agents_v
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS pob, (
    SELECT
      *
    FROM silver_bec_ods.fnd_lookup_values
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS ml1, (
    SELECT
      *
    FROM silver_bec_ods.fnd_lookup_values
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS ml2, (
    SELECT
      *
    FROM silver_bec_ods.fnd_lookup_values
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS ml3, safety_stock AS mss, mtl_planners AS mp, onhand_qty AS moh, onhand_qty AS moh_nn
  WHERE
    msi.inventory_item_id = cic.INVENTORY_ITEM_ID()
    AND msi.organization_id = cic.ORGANIZATION_ID()
    AND msi.buyer_id = pob.AGENT_ID()
    AND msi.wip_supply_type = ml1.LOOKUP_CODE()
    AND msi.inventory_item_id = mss.INVENTORY_ITEM_ID()
    AND msi.organization_id = mss.ORGANIZATION_ID()
    AND ml1.LOOKUP_TYPE() = 'WIP_SUPPLY'
    AND msi.inventory_planning_code = ml2.LOOKUP_CODE()
    AND ml2.LOOKUP_TYPE() = 'MTL_MATERIAL_PLANNING'
    AND msi.mrp_planning_code = ml3.LOOKUP_CODE()
    AND ml3.LOOKUP_TYPE() = 'MRP_PLANNING_CODE'
    AND cic.COST_TYPE_ID() = 1
    AND msi.planner_code = mp.PLANNER_CODE()
    AND msi.organization_id = mp.ORGANIZATION_ID()
    AND msi.organization_id = moh.MOQ_ORG_ID()
    AND msi.inventory_item_id = moh.MOQ_ITEM_ID()
    AND moh.AVAILABILITY_TYPE() = 1
    AND msi.organization_id = moh_nn.MOQ_ORG_ID()
    AND msi.inventory_item_id = moh_nn.MOQ_ITEM_ID()
    AND moh_nn.AVAILABILITY_TYPE() = 2
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_ascp_plan_params' AND batch_name = 'ascp';