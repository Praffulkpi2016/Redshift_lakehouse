TRUNCATE table gold_bec_dwh.FACT_CST_ONHAND_QTY;
WITH CTE_CST_STANDARD_COSTS_U3 AS (
  SELECT
    standard_cost,
    inventory_item_id,
    organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_standard_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS c
  WHERE
    standard_cost_revision_date <= CURRENT_TIMESTAMP()
    AND cost_update_id = (
      SELECT
        MAX(cost_update_id)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_standard_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS c1
      WHERE
        inventory_item_id = c.inventory_item_id
        AND organization_id = c.organization_id
        AND standard_cost_revision_date <= CURRENT_TIMESTAMP()
    )
), CTE_CST_ELEMENTAL_COSTS_U3 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_MATERIAL_COSTS_U3 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 1
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_MATERIAL_OVERHEAD_COSTS_U3 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 2
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_RESOURCE_COSTS_U3 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 3
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_OUTSIDE_PROCESSING_COSTS_U3 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 4
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_OVERHEAD_COSTS_U3 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 5
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_STANDARD_COSTS_U1 AS (
  SELECT
    standard_cost,
    inventory_item_id,
    organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_standard_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS c
  WHERE
    standard_cost_revision_date <= CURRENT_TIMESTAMP()
    AND cost_update_id = (
      SELECT
        MAX(cost_update_id)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_standard_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS c1
      WHERE
        inventory_item_id = c.inventory_item_id
        AND organization_id = c.organization_id
        AND standard_cost_revision_date <= CURRENT_TIMESTAMP()
    )
), CTE_CST_ELEMENTAL_COSTS_U1 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_MATERIAL_COSTS_U1 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 1
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_MATERIAL_OVERHEAD_COSTS_U1 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 2
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_RESOURCE_COSTS_U1 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 3
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_OUTSIDE_PROCESSING_COSTS_U1 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 4
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_OVERHEAD_COSTS_U1 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 5
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_STANDARD_COSTS_U2 AS (
  SELECT
    standard_cost,
    inventory_item_id,
    organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_standard_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS c
  WHERE
    standard_cost_revision_date <= CURRENT_TIMESTAMP()
    AND cost_update_id = (
      SELECT
        MAX(cost_update_id)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_standard_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS c1
      WHERE
        inventory_item_id = c.inventory_item_id
        AND organization_id = c.organization_id
        AND standard_cost_revision_date <= CURRENT_TIMESTAMP()
    )
), CTE_CST_ELEMENTAL_COSTS_U2 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION2_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_MATERIAL_COSTS_U2 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION2_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 1
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_MATERIAL_OVERHEAD_COSTS_U2 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION2_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 2
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_RESOURCE_COSTS_U2 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION2_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 3
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_OUTSIDE_PROCESSING_COSTS_U2 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION2_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 4
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
), CTE_CST_OVERHEAD_COSTS_U2 AS (
  SELECT
    SUM(e.standard_cost) AS standard_cost,
    e.inventory_item_id,
    e.organization_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.cst_elemental_costs
    WHERE
      is_deleted_flg <> 'Y'
  ) AS e, (
    SELECT DISTINCT
      inventory_item_id,
      organization_id
    FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION2_STG
  ) AS mmt
  WHERE
    1 = 1
    AND e.cost_element_id = 5
    AND e.inventory_item_id = mmt.inventory_item_id
    AND e.organization_id = mmt.organization_id
    AND e.creation_date = (
      SELECT
        MAX(creation_date)
      FROM (
        SELECT
          *
        FROM silver_bec_ods.cst_elemental_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS e1
      WHERE
        e1.inventory_item_id = mmt.inventory_item_id
        AND e1.organization_id = mmt.organization_id
        AND e1.creation_date <= CURRENT_TIMESTAMP()
    )
  GROUP BY
    e.inventory_item_id,
    e.organization_id
)
INSERT INTO gold_bec_dwh.FACT_CST_ONHAND_QTY
(
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY dw_load_id) AS ROWNUMBER
  FROM (
    SELECT
      part_number,
      cat_seg1,
      cat_seg2,
      category,
      organization_name,
      operating_unit,
      description,
      unit_of_measure,
      inventory_item_status_code,
      planning_make_buy_code,
      mrp_planning_code,
      quantity,
      material_cost,
      material_overhead_cost,
      resource_cost,
      outside_processing_cost,
      overhead_cost,
      item_cost,
      COALESCE(material_cost, 0) * COALESCE(quantity, 0) AS Ext_Material_Cost,
      COALESCE(material_overhead_cost, 0) * COALESCE(quantity, 0) AS Ext_material_overhead_cost,
      COALESCE(resource_cost, 0) * COALESCE(quantity, 0) AS Ext_resource_cost,
      COALESCE(outside_processing_cost, 0) * COALESCE(quantity, 0) AS Ext_outside_processing_cost,
      COALESCE(overhead_cost, 0) * COALESCE(quantity, 0) AS Ext_overhead_cost,
      COALESCE(item_cost, 0) * COALESCE(quantity, 0) AS Extended_cost,
      subinventory,
      locator,
      serial_number,
      lot_number,
      date_received,
      transaction_type_id,
      transaction_source_type_id,
      organization_id,
      source_date_aging,
      material_account,
      subinventory_type,
      subinventory_category,
      subinventory_subcategory,
      uom_code,
      vmi_flag,
      (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || organization_id AS organization_id_KEY,
      (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || transaction_type_id AS transaction_type_id_KEY,
      (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || transaction_source_type_id AS transaction_source_type_id_KEY,
      'N' AS is_deleted_flg, /* audit columns */
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
      ) || '-' || COALESCE(part_number, 'NA') || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(locator, 'NA') || '-' || COALESCE(serial_number, 'NA') || '-' || COALESCE(lot_number, 'NA') || '-' || COALESCE(date_received, '1900-01-01 12:00:00') || '-' || COALESCE(subinventory, 'NA') || '-' || COALESCE(quantity, 0) AS dw_load_id,
      CURRENT_TIMESTAMP() AS dw_insert_date,
      CURRENT_TIMESTAMP() AS dw_update_date
    FROM (
      (
        SELECT
          mmt.segment1 AS part_number,
          mmt.cat_seg1,
          mmt.cat_seg2,
          mmt.category,
          mmt.organization_name,
          mmt.operating_unit AS operating_unit,
          mmt.description,
          mmt.primary_unit_of_measure AS unit_of_measure,
          mmt.primary_uom_code AS uom_code,
          mmt.inventory_item_status_code,
          CASE
            WHEN mmt.planning_make_buy_code = 1
            THEN 'Make'
            WHEN mmt.planning_make_buy_code = 2
            THEN 'Buy'
          END AS planning_make_buy_code,
          mmt.mrp_planning_code,
          (
            SELECT
              SUM(primary_transaction_quantity)
            FROM (
              SELECT
                *
              FROM silver_bec_ods.mtl_onhand_quantities_detail
              WHERE
                is_deleted_flg <> 'Y'
            )
            WHERE
              inventory_item_id = mmt.inventory_item_id
              AND organization_id = mmt.organization_id
              AND subinventory_code = mmt.subinventory_code
              AND COALESCE(locator_id, -1) = COALESCE(mmt.locator_id, -1)
              AND create_transaction_id = mmt.create_transaction_id
              AND COALESCE(lot_number, '@#$') = COALESCE(mmt.lot_number, '@#$')
              AND date_received <= CURRENT_TIMESTAMP()
              AND last_update_date <= CURRENT_TIMESTAMP()
          ) AS quantity,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccmc.standard_cost END
          ) AS material_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccmoc.standard_cost END
          ) AS material_overhead_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccrc.standard_cost END
          ) AS resource_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccopc.standard_cost END
          ) AS outside_processing_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccoc.standard_cost END
          ) AS overhead_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccec.standard_cost END
          ) AS item_cost,
          mmt.transaction_date AS date_received,
          DATEDIFF(
            CURRENT_TIMESTAMP(),
            COALESCE(
              COALESCE(
                (
                  CASE
                    WHEN mmt.planning_make_buy_code = 2
                    THEN (
                      SELECT
                        (
                          MAX(t1.transaction_date)
                        )
                      FROM (
                        SELECT
                          *
                        FROM silver_bec_ods.mtl_transaction_lot_numbers
                        WHERE
                          is_deleted_flg <> 'Y'
                      ) AS l1, gold_bec_dwh.FACT_CST_ONHAND_QTY_STG AS t1
                      WHERE
                        l1.transaction_id = t1.transaction_id
                        AND t1.transaction_type_id IN (18)
                        AND t1.inventory_item_id = mmt.inventory_item_id
                        AND (
                          COALESCE(l1.lot_number, '@#$') = COALESCE(mmt.lot_number, '#$%')
                        )
                    )
                    ELSE (
                      SELECT
                        (
                          MAX(t1.transaction_date)
                        )
                      FROM (
                        SELECT
                          *
                        FROM silver_bec_ods.mtl_transaction_lot_numbers
                        WHERE
                          is_deleted_flg <> 'Y'
                      ) AS l1, gold_bec_dwh.FACT_CST_ONHAND_QTY_STG AS t1
                      WHERE
                        l1.transaction_id = t1.transaction_id
                        AND t1.transaction_type_id IN (44)
                        AND t1.inventory_item_id = mmt.inventory_item_id
                        AND (
                          COALESCE(l1.lot_number, '@#$') = COALESCE(mmt.lot_number, '#$%')
                        )
                    )
                  END
                ),
                (
                  SELECT
                    (
                      MAX(t1.transaction_date)
                    )
                  FROM (
                    SELECT
                      *
                    FROM silver_bec_ods.mtl_transaction_lot_numbers
                    WHERE
                      is_deleted_flg <> 'Y'
                  ) AS l1, gold_bec_dwh.FACT_CST_ONHAND_QTY_STG AS t1
                  WHERE
                    l1.transaction_id = t1.transaction_id
                    AND t1.transaction_type_id IN (40, 41, 42) /*                                            AND t1.organization_id = */
                    AND t1.inventory_item_id = mmt.inventory_item_id
                    AND (
                      COALESCE(l1.lot_number, '@#$') = COALESCE(mmt.lot_number, '#$%')
                    )
                )
              ),
              mmt.transaction_date
            ) - 1
          ) AS source_date_aging,
          mmt.organization_id,
          mmt.transaction_source_type_id,
          mmt.transaction_type_id,
          mmt.lot_number,
          NULL AS serial_number,
          mmt.locator,
          mmt.subinventory_code AS subinventory,
          mmt.material_account,
          CASE
            WHEN CAST(mmt.asset_inventory AS STRING) = 1
            THEN 'Asset'
            WHEN CAST(mmt.asset_inventory AS STRING) = 2
            THEN 'Expense'
            ELSE CAST(asset_inventory AS STRING)
          END AS subinventory_type,
          mmt.attribute10 AS subinventory_category,
          mmt.attribute11 AS subinventory_subcategory,
          mmt.vmi_flag
        FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION1_STG AS mmt, CTE_CST_STANDARD_COSTS_U1 AS ccsc, CTE_CST_ELEMENTAL_COSTS_U1 AS ccec, CTE_CST_MATERIAL_COSTS_U1 AS ccmc, CTE_CST_MATERIAL_OVERHEAD_COSTS_U1 AS ccmoc, CTE_CST_RESOURCE_COSTS_U1 AS ccrc, CTE_CST_OUTSIDE_PROCESSING_COSTS_U1 AS ccopc, CTE_CST_OVERHEAD_COSTS_U1 AS ccoc
        WHERE
          ccsc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccsc.ORGANIZATION_ID() = mmt.organization_id
          AND ccec.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccec.ORGANIZATION_ID() = mmt.organization_id
          AND ccmc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccmc.ORGANIZATION_ID() = mmt.organization_id
          AND ccmoc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccmoc.ORGANIZATION_ID() = mmt.organization_id
          AND ccrc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccrc.ORGANIZATION_ID() = mmt.organization_id
          AND ccopc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccopc.ORGANIZATION_ID() = mmt.organization_id
          AND ccoc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccoc.ORGANIZATION_ID() = mmt.organization_id
      )
      UNION ALL
      (
        SELECT
          mmt.segment1 AS part_number,
          mmt.cat_seg1,
          mmt.cat_seg2,
          mmt.category,
          mmt.organization_name,
          mmt.operating_unit AS operating_unit,
          mmt.description,
          mmt.primary_unit_of_measure AS unit_of_measure,
          mmt.primary_uom_code AS uom_code,
          mmt.inventory_item_status_code,
          CASE
            WHEN mmt.planning_make_buy_code = 1
            THEN 'Make'
            WHEN mmt.planning_make_buy_code = 2
            THEN 'Buy'
          END AS planning_make_buy_code,
          mmt.mrp_planning_code,
          1 AS quantity,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccmc.standard_cost END
          ) AS material_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccmoc.standard_cost END
          ) AS material_overhead_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccrc.standard_cost END
          ) AS resource_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccopc.standard_cost END
          ) AS outside_processing_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccoc.standard_cost END
          ) AS overhead_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccec.standard_cost END
          ) AS item_cost,
          mmt.transaction_date AS date_received,
          FLOOR(DATEDIFF(CURRENT_TIMESTAMP(), mmt.initialization_date)) - 1 AS source_date_aging,
          mmt.organization_id,
          mmt.transaction_source_type_id,
          mmt.transaction_type_id,
          mmt.lot_number,
          mmt.serial_number AS serial_number,
          mmt.locator,
          mmt.current_subinventory_code AS subinventory,
          mmt.material_account,
          CASE
            WHEN CAST(mmt.asset_inventory AS STRING) = 1
            THEN 'Asset'
            WHEN CAST(mmt.asset_inventory AS STRING) = 2
            THEN 'Expense'
            ELSE CAST(asset_inventory AS STRING)
          END AS subinventory_type,
          mmt.attribute10 AS subinventory_category,
          mmt.attribute11 AS subinventory_subcategory,
          mmt.vmi_flag
        FROM gold_bec_dwh.fact_cst_onhand_qty_union2_stg AS mmt, CTE_CST_STANDARD_COSTS_U2 AS ccsc, CTE_CST_ELEMENTAL_COSTS_U2 AS ccec, CTE_CST_MATERIAL_COSTS_U2 AS ccmc, CTE_CST_MATERIAL_OVERHEAD_COSTS_U2 AS ccmoc, CTE_CST_RESOURCE_COSTS_U2 AS ccrc, CTE_CST_OUTSIDE_PROCESSING_COSTS_U2 AS ccopc, CTE_CST_OVERHEAD_COSTS_U2 AS ccoc
        WHERE
          ccsc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccsc.ORGANIZATION_ID() = mmt.organization_id
          AND ccec.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccec.ORGANIZATION_ID() = mmt.organization_id
          AND ccmc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccmc.ORGANIZATION_ID() = mmt.organization_id
          AND ccmoc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccmoc.ORGANIZATION_ID() = mmt.organization_id
          AND ccrc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccrc.ORGANIZATION_ID() = mmt.organization_id
          AND ccopc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccopc.ORGANIZATION_ID() = mmt.organization_id
          AND ccoc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccoc.ORGANIZATION_ID() = mmt.organization_id
      )
      UNION ALL
      (
        SELECT
          mmt.segment1 AS part_number,
          mmt.cat_seg1,
          mmt.cat_seg2,
          mmt.category,
          mmt.organization_name,
          mmt.operating_unit AS operating_unit,
          mmt.description,
          mmt.primary_unit_of_measure AS unit_of_measure,
          mmt.primary_uom_code AS uom_code,
          mmt.inventory_item_status_code,
          CASE
            WHEN mmt.planning_make_buy_code = 1
            THEN 'Make'
            WHEN mmt.planning_make_buy_code = 2
            THEN 'Buy'
          END AS planning_make_buy_code,
          mmt.mrp_planning_code,
          1 AS quantity,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccmc.standard_cost END
          ) AS material_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccmoc.standard_cost END
          ) AS material_overhead_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccrc.standard_cost END
          ) AS resource_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccopc.standard_cost END
          ) AS outside_processing_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccoc.standard_cost END
          ) AS overhead_cost,
          (
            CASE WHEN ccsc.standard_cost = 0 THEN 0 ELSE ccec.standard_cost END
          ) AS item_cost,
          mmt.transaction_date AS date_received,
          FLOOR(DATEDIFF(CURRENT_TIMESTAMP(), mmt.initialization_date)) - 1 AS source_date_aging,
          mmt.organization_id,
          mmt.transaction_source_type_id,
          mmt.transaction_type_id,
          mmt.lot_number,
          mmt.serial_number AS serial_number,
          mmt.locator,
          mmt.current_subinventory_code AS subinventory,
          mmt.material_account,
          CASE
            WHEN CAST(mmt.asset_inventory AS STRING) = 1
            THEN 'Asset'
            WHEN CAST(mmt.asset_inventory AS STRING) = 2
            THEN 'Expense'
            ELSE CAST(asset_inventory AS STRING)
          END AS subinventory_type,
          mmt.attribute10 AS subinventory_category,
          mmt.attribute11 AS subinventory_subcategory,
          mmt.vmi_flag
        FROM gold_bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG AS mmt, CTE_CST_STANDARD_COSTS_U3 AS ccsc, CTE_CST_ELEMENTAL_COSTS_U3 AS ccec, CTE_CST_MATERIAL_COSTS_U3 AS ccmc, CTE_CST_MATERIAL_OVERHEAD_COSTS_U3 AS ccmoc, CTE_CST_RESOURCE_COSTS_U3 AS ccrc, CTE_CST_OUTSIDE_PROCESSING_COSTS_U3 AS ccopc, CTE_CST_OVERHEAD_COSTS_U3 AS ccoc
        WHERE
          ccsc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccsc.ORGANIZATION_ID() = mmt.organization_id
          AND ccec.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccec.ORGANIZATION_ID() = mmt.organization_id
          AND ccmc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccmc.ORGANIZATION_ID() = mmt.organization_id
          AND ccmoc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccmoc.ORGANIZATION_ID() = mmt.organization_id
          AND ccrc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccrc.ORGANIZATION_ID() = mmt.organization_id
          AND ccopc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccopc.ORGANIZATION_ID() = mmt.organization_id
          AND ccoc.INVENTORY_ITEM_ID() = mmt.inventory_item_id
          AND ccoc.ORGANIZATION_ID() = mmt.organization_id
      )
    )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_cst_onhand_qty' AND batch_name = 'costing';