DROP table IF EXISTS gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION1_STG;
CREATE TABLE gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION1_STG AS
(
  /* ----------------------------------------------------------------------------------	*/
  WITH wro AS (
    SELECT
      inventory_item_id,
      organization_id,
      wip_entity_id,
      operation_seq_num,
      quantity_per_assembly,
      required_quantity,
      quantity_issued,
      wip_supply_type
    FROM (
      SELECT
        *
      FROM silver_bec_ods.WIP_REQUIREMENT_OPERATIONS
      WHERE
        is_deleted_flg <> 'Y'
    )
    WHERE
      operation_seq_num >= 0
  ) /*		and last_update_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info */ /*		where dw_table_name = 'fact_wo_value_variance_union1_stg' */ /*		and batch_name = 'wip') */, wdj /* ----------------------------------------------------------------------------------	*/ AS (
    SELECT
      primary_item_id,
      organization_id,
      wip_entity_id,
      status_type,
      date_released,
      date_completed,
      date_closed,
      scheduled_start_date,
      class_code,
      quantity_completed,
      quantity_scrapped,
      start_quantity
    FROM (
      SELECT
        *
      FROM silver_bec_ods.wip_discrete_jobs
      WHERE
        is_deleted_flg <> 'Y'
    )
    WHERE
      1 = 1
  ) /*	and last_update_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info */ /*	where dw_table_name = 'fact_wo_value_variance_union1_stg' */ /*	and batch_name = 'wip') */, we /* ---------------------------------------------------------------------------------- */ AS (
    SELECT
      organization_id,
      primary_item_id,
      wip_entity_id,
      wip_entity_name,
      description
    FROM (
      SELECT
        *
      FROM silver_bec_ods.wip_entities
      WHERE
        is_deleted_flg <> 'Y'
    )
    WHERE
      1 = 1
  ) /* and last_update_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info */ /* where dw_table_name = 'fact_wo_value_variance_union1_stg' */ /* and batch_name = 'wip') */, csc /* ---------------------------------------------------------------------------------- */ AS (
    SELECT
      inventory_item_id,
      organization_id,
      cost_update_id,
      standard_cost_revision_date,
      standard_cost
    FROM (
      SELECT
        *
      FROM silver_bec_ods.CST_STANDARD_COSTS
      WHERE
        is_deleted_flg <> 'Y'
    )
    WHERE
      1 = 1
  ) /* and last_update_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info */ /* where dw_table_name = 'fact_wo_value_variance_union1_stg' */ /* and batch_name = 'wip') */, csc_max_id /* -----------------------------------------------------------------		*/ AS (
    SELECT
      MAX(c2.cost_update_id) AS max_cost_update_id,
      c2.inventory_item_id,
      c2.organization_id
    FROM csc AS c2
    INNER JOIN wro AS r
      ON COALESCE(c2.inventory_item_id, 0) = COALESCE(r.inventory_item_id, 0)
      AND COALESCE(c2.organization_id, 0) = COALESCE(r.organization_id, 0)
    INNER JOIN wdj AS d
      ON COALESCE(d.wip_entity_id, 0) = COALESCE(r.wip_entity_id, 0)
      AND d.organization_id = r.organization_id
      AND c2.standard_cost_revision_date <= COALESCE(d.date_closed, (
        CURRENT_TIMESTAMP()
      ))
    GROUP BY
      c2.inventory_item_id,
      c2.organization_id
  ), cec /* ----------------------------------------------------------------- */ AS (
    SELECT
      inventory_item_id,
      organization_id,
      cost_update_id,
      cost_element_id,
      creation_date,
      standard_cost
    FROM (
      SELECT
        *
      FROM silver_bec_ods.CST_ELEMENTAL_COSTS
      WHERE
        is_deleted_flg <> 'Y'
    )
  ), cec_max_date /* ---------------------------------------------------------------------------------- */ AS (
    SELECT
      MAX(e2.creation_date) AS max_creation_date,
      e2.inventory_item_id,
      e2.organization_id,
      d.wip_entity_id
    FROM cec AS e2
    INNER JOIN wro AS r
      ON COALESCE(e2.inventory_item_id, 0) = COALESCE(r.inventory_item_id, 0)
      AND COALESCE(e2.organization_id, 0) = COALESCE(r.organization_id, 0)
    INNER JOIN wdj AS d
      ON COALESCE(d.wip_entity_id, 0) = COALESCE(r.wip_entity_id, 0)
      AND d.organization_id = r.organization_id
      AND e2.creation_date <= COALESCE(d.date_closed, (
        CURRENT_TIMESTAMP()
      ))
    GROUP BY
      e2.inventory_item_id,
      e2.organization_id,
      d.wip_entity_id
  ), cte1 /* ---------------------------------------------------------------------------------- */ AS (
    SELECT
      c1.inventory_item_id,
      c1.organization_id,
      c1.cost_update_id,
      d.wip_entity_id,
      c1.standard_cost
    FROM csc AS c1
    INNER JOIN wro AS r
      ON COALESCE(c1.inventory_item_id, 0) = COALESCE(r.inventory_item_id, 0)
      AND COALESCE(c1.organization_id, 0) = COALESCE(r.organization_id, 0)
    INNER JOIN wdj AS d
      ON d.wip_entity_id = r.wip_entity_id
      AND d.organization_id = r.organization_id
      AND c1.standard_cost_revision_date <= COALESCE(d.date_closed, (
        CURRENT_TIMESTAMP()
      ))
      AND c1.cost_update_id IN (
        SELECT
          max_cost_update_id
        FROM csc_max_id AS c2
        WHERE
          inventory_item_id = c1.inventory_item_id AND organization_id = c1.organization_id
      )
    GROUP BY
      c1.inventory_item_id,
      c1.organization_id,
      c1.cost_update_id,
      d.wip_entity_id,
      c1.standard_cost
  ), cte2 /* ---------------------------------------------------------------------------------- */ AS (
    SELECT
      SUM(e1.standard_cost) AS sum_standard_cost,
      e1.inventory_item_id,
      e1.organization_id,
      d.wip_entity_id
    FROM (
      SELECT
        inventory_item_id,
        organization_id,
        creation_date,
        standard_cost
      FROM cec
    ) AS e1
    INNER JOIN wro AS r
      ON e1.inventory_item_id = r.inventory_item_id AND e1.organization_id = r.organization_id
    INNER JOIN wdj AS d
      ON d.wip_entity_id = r.wip_entity_id AND d.organization_id = r.organization_id
    WHERE
      e1.creation_date IN (
        SELECT
          max_creation_date
        FROM cec_max_date
        WHERE
          inventory_item_id = e1.inventory_item_id
          AND organization_id = e1.organization_id
          AND wip_entity_id = d.wip_entity_id
      )
    GROUP BY
      e1.inventory_item_id,
      e1.organization_id,
      d.wip_entity_id
  ), cte3 /* ---------------------------------------------------------------------------------- */ AS (
    SELECT
      SUM(e1.standard_cost) AS sum_standard_cost,
      e1.inventory_item_id,
      e1.organization_id,
      d.wip_entity_id
    FROM (
      SELECT
        inventory_item_id,
        organization_id,
        creation_date,
        standard_cost
      FROM cec
      WHERE
        cost_element_id = 1
    ) AS e1
    INNER JOIN wro AS r
      ON COALESCE(e1.inventory_item_id, 0) = COALESCE(r.inventory_item_id, 0)
      AND COALESCE(e1.organization_id, 0) = COALESCE(r.organization_id, 0)
    INNER JOIN wdj AS d
      ON d.wip_entity_id = r.wip_entity_id AND d.organization_id = r.organization_id
    WHERE
      e1.creation_date IN (
        SELECT
          max_creation_date
        FROM cec_max_date
        WHERE
          inventory_item_id = e1.inventory_item_id
          AND organization_id = e1.organization_id
          AND wip_entity_id = d.wip_entity_id
      )
    GROUP BY
      e1.inventory_item_id,
      e1.organization_id,
      d.wip_entity_id
  ), cte4 /* ---------------------------------------------------------------------------------- */ AS (
    SELECT
      SUM(e1.standard_cost) AS sum_standard_cost,
      e1.inventory_item_id,
      e1.organization_id,
      d.wip_entity_id
    FROM (
      SELECT
        inventory_item_id,
        organization_id,
        creation_date,
        standard_cost
      FROM cec
      WHERE
        cost_element_id = 2
    ) AS e1
    INNER JOIN wro AS r
      ON e1.inventory_item_id = r.inventory_item_id AND e1.organization_id = r.organization_id
    INNER JOIN wdj AS d
      ON d.wip_entity_id = r.wip_entity_id AND d.organization_id = r.organization_id
    WHERE
      e1.creation_date IN (
        SELECT
          max_creation_date
        FROM cec_max_date
        WHERE
          inventory_item_id = e1.inventory_item_id
          AND organization_id = e1.organization_id
          AND wip_entity_id = d.wip_entity_id
      )
    GROUP BY
      e1.inventory_item_id,
      e1.organization_id,
      d.wip_entity_id
  ), cte5 /* ---------------------------------------------------------------------------------- */ AS (
    SELECT
      SUM(e1.standard_cost) AS sum_standard_cost,
      e1.inventory_item_id,
      e1.organization_id,
      d.wip_entity_id
    FROM (
      SELECT
        inventory_item_id,
        organization_id,
        creation_date,
        standard_cost
      FROM cec
      WHERE
        cost_element_id = 3
    ) AS e1
    INNER JOIN wro AS r
      ON e1.inventory_item_id = r.inventory_item_id AND e1.organization_id = r.organization_id
    INNER JOIN wdj AS d
      ON d.wip_entity_id = r.wip_entity_id AND d.organization_id = r.organization_id
    WHERE
      e1.creation_date IN (
        SELECT
          max_creation_date
        FROM cec_max_date
        WHERE
          organization_id = e1.organization_id
          AND inventory_item_id = e1.inventory_item_id
          AND wip_entity_id = d.wip_entity_id
      )
    GROUP BY
      e1.inventory_item_id,
      e1.organization_id,
      d.wip_entity_id
  ), cte6 /* ---------------------------------------------------------------------------------- */ AS (
    SELECT
      SUM(e1.standard_cost) AS sum_standard_cost,
      e1.inventory_item_id,
      e1.organization_id,
      d.wip_entity_id
    FROM (
      SELECT
        inventory_item_id,
        organization_id,
        creation_date,
        standard_cost
      FROM cec
      WHERE
        cost_element_id = 4
    ) AS e1
    INNER JOIN wro AS r
      ON e1.inventory_item_id = r.inventory_item_id AND e1.organization_id = r.organization_id
    INNER JOIN wdj AS d
      ON d.wip_entity_id = r.wip_entity_id AND d.organization_id = r.organization_id
    WHERE
      e1.creation_date IN (
        SELECT
          max_creation_date
        FROM cec_max_date
        WHERE
          organization_id = e1.organization_id
          AND inventory_item_id = e1.inventory_item_id
          AND wip_entity_id = d.wip_entity_id
      )
    GROUP BY
      e1.inventory_item_id,
      e1.organization_id,
      d.wip_entity_id
  ), cte7 /* ---------------------------------------------------------------------------------- */ AS (
    SELECT
      SUM(e1.standard_cost) AS sum_standard_cost,
      e1.inventory_item_id,
      e1.organization_id,
      d.wip_entity_id
    FROM (
      SELECT
        inventory_item_id,
        organization_id,
        creation_date,
        standard_cost
      FROM cec
      WHERE
        cost_element_id = 5
    ) AS e1
    INNER JOIN wro AS r
      ON e1.inventory_item_id = r.inventory_item_id AND e1.organization_id = r.organization_id
    INNER JOIN wdj AS d
      ON d.wip_entity_id = r.wip_entity_id AND d.organization_id = r.organization_id
    WHERE
      e1.creation_date IN (
        SELECT
          max_creation_date
        FROM cec_max_date
        WHERE
          organization_id = e1.organization_id
          AND inventory_item_id = e1.inventory_item_id
          AND wip_entity_id = d.wip_entity_id
      )
    GROUP BY
      e1.inventory_item_id,
      e1.organization_id,
      d.wip_entity_id
  )
  SELECT
    organization_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || organization_id AS organization_id_key,
    primary_item_id,
    inventory_item_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || inventory_item_id AS inventory_item_id_key,
    wip_entity_id,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || wip_entity_id AS wip_entity_id_key,
    work_order,
    wo_description,
    assembly_item,
    ASSEMBLY_DESCRIPTION,
    status_type, /* WO_Status, */
    CAST(start_quantity AS INT) AS start_quantity,
    CAST(quantity_completed AS INT) AS quantity_completed,
    CAST(quantity_scrapped AS INT) AS quantity_scrapped,
    date_released,
    date_completed,
    date_closed,
    JOB_START_DATE,
    class_code,
    item_number,
    item_description,
    CAST(operation_seq_num AS INT) AS operation_seq_num,
    quantity_per_assembly,
    required_quantity,
    quantity_issued,
    WIP_Supply_Type,
    CAST(department_code AS STRING) AS department_code,
    CAST(res_op_seq_num AS INT) AS res_op_seq_num,
    CAST(resource_seq_num AS INT) AS resource_seq_num,
    CAST(resource_code AS STRING) AS resource_code,
    CAST(res_uom_code AS STRING) AS res_uom_code,
    CAST(Res_usage_rate AS INT) AS Res_usage_rate,
    CAST(applied_resource_units AS INT) AS applied_resource_units,
    item_transaction_value,
    item_cost_at_close,
    unit_item_cost_at_close,
    mtl_transaction_value,
    mtl_cost_at_close,
    unit_mtl_cost_at_close,
    mtl_oh_transaction_value,
    mtl_oh_cost_at_close,
    unit_mtl_oh_cost_at_close,
    res_transaction_value,
    res_cost_at_close,
    unit_res_cost_at_close,
    osp_transaction_value,
    osp_cost_at_close,
    unit_osp_cost_at_close,
    oh_transaction_value,
    oh_cost_at_close,
    unit_oh_cost_at_close,
    CAST(cost_update_txn_value AS INT) AS cost_update_txn_value,
    CAST(Mtl_variance AS INT) AS Mtl_variance,
    CAST(mtl_oh_variance AS INT) AS mtl_oh_variance,
    CAST(Res_variance AS INT) AS Res_variance,
    CAST(osp_variance AS INT) AS osp_variance,
    CAST(oh_variance AS INT) AS oh_variance,
    CAST(net_value AS INT) AS net_value,
    organization_id1,
    ALTERNATE_BOM,
    COMMENTS,
    CASE
      WHEN comments = 'Work Order Components'
      THEN (
        COALESCE(CAST(mtl_transaction_value AS INT), 0) - COALESCE(CAST(item_cost_at_close AS INT), 0)
      )
    END AS mtl_value_difference,
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
    ) || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(wip_entity_id, 0) || '-' || COALESCE(CAST(operation_seq_num AS INT), 0) || '-' || COALESCE(CAST(res_op_seq_num AS INT), 0) || '-' || COALESCE(CAST(resource_seq_num AS INT), 0) || '-' || COALESCE(WIP_Supply_Type, 'NA') || '-' || COALESCE(ALTERNATE_BOM, 'NA') || '-' || COALESCE(comments, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      e.organization_id,
      e.primary_item_id,
      r.inventory_item_id,
      e.wip_entity_id,
      e.wip_entity_name AS work_order,
      e.description AS wo_description,
      (
        SELECT
          segment1
        FROM (
          SELECT
            *
          FROM silver_bec_ods.mtl_system_items_b
          WHERE
            is_deleted_flg <> 'Y'
        )
        WHERE
          organization_id = e.organization_id AND inventory_item_id = e.primary_item_id
      ) AS assembly_item,
      (
        SELECT
          description
        FROM (
          SELECT
            *
          FROM silver_bec_ods.mtl_system_items_b
          WHERE
            is_deleted_flg <> 'Y'
        )
        WHERE
          organization_id = e.organization_id AND inventory_item_id = e.primary_item_id
      ) AS ASSEMBLY_DESCRIPTION,
      d.status_type,
      NULL AS start_quantity,
      NULL AS quantity_completed,
      NULL AS quantity_scrapped,
      d.date_released,
      d.date_completed,
      d.date_closed,
      d.scheduled_start_date AS JOB_START_DATE,
      d.class_code,
      (
        SELECT
          segment1
        FROM (
          SELECT
            *
          FROM silver_bec_ods.mtl_system_items_b
          WHERE
            is_deleted_flg <> 'Y'
        )
        WHERE
          organization_id = r.organization_id AND inventory_item_id = r.inventory_item_id
      ) AS item_number,
      (
        SELECT
          description
        FROM (
          SELECT
            *
          FROM silver_bec_ods.mtl_system_items_b
          WHERE
            is_deleted_flg <> 'Y'
        )
        WHERE
          organization_id = r.organization_id AND inventory_item_id = r.inventory_item_id
      ) AS item_description,
      r.operation_seq_num,
      r.quantity_per_assembly,
      r.required_quantity,
      r.quantity_issued,
      CAST(CASE
        WHEN CAST(CAST(r.wip_supply_type AS INT) AS STRING) = 1
        THEN 'Push'
        WHEN CAST(CAST(r.wip_supply_type AS INT) AS STRING) = 2
        THEN 'Assembly Pull'
        WHEN CAST(CAST(r.wip_supply_type AS INT) AS STRING) = 3
        THEN 'Operation Pull'
        WHEN CAST(CAST(r.wip_supply_type AS INT) AS STRING) = 4
        THEN 'Bulk'
        WHEN CAST(CAST(r.wip_supply_type AS INT) AS STRING) = 5
        THEN 'Vendor'
        WHEN CAST(CAST(r.wip_supply_type AS INT) AS STRING) = 6
        THEN 'Phantom'
        WHEN CAST(CAST(r.wip_supply_type AS INT) AS STRING) = 7
        THEN 'Based on Bill'
        ELSE CAST(CAST(r.wip_supply_type AS INT) AS STRING)
      END AS STRING) AS WIP_Supply_Type,
      NULL AS department_code,
      NULL AS res_op_seq_num,
      NULL AS resource_seq_num,
      NULL AS resource_code,
      NULL AS res_uom_code,
      NULL AS Res_usage_rate,
      NULL AS applied_resource_units,
      (
        SELECT
          SUM(base_transaction_value)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.MTL_TRANSACTION_ACCOUNTS
          WHERE
            is_deleted_flg <> 'Y'
        ) AS a, (
          SELECT
            *
          FROM silver_bec_ods.mtl_material_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS t
        WHERE
          a.transaction_id = t.transaction_id
          AND t.transaction_source_type_id = 5
          AND t.transaction_source_id = e.wip_entity_id
          AND r.inventory_item_id = t.inventory_item_id
          AND a.accounting_line_type = 7
      ) AS item_transaction_value, /* y.transaction_type_name, */
      (
        (
          CASE WHEN cte1.standard_cost = 0 THEN 0 ELSE cte2.sum_standard_cost END
        ) * r.required_quantity
      ) AS item_cost_at_close,
      CASE WHEN cte1.standard_cost = 0 THEN 0 ELSE cte2.sum_standard_cost END AS unit_item_cost_at_close,
      (
        SELECT
          SUM(base_transaction_value)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.MTL_TRANSACTION_ACCOUNTS
          WHERE
            is_deleted_flg <> 'Y'
        ) AS a, (
          SELECT
            *
          FROM silver_bec_ods.mtl_material_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS t
        WHERE
          a.transaction_id = t.transaction_id
          AND t.transaction_source_type_id = 5
          AND t.transaction_source_id = e.wip_entity_id
          AND r.inventory_item_id = t.inventory_item_id
          AND A.COST_ELEMENT_ID = 1
          AND a.accounting_line_type = 7
      ) AS mtl_transaction_value,
      (
        (
          CASE WHEN cte1.standard_cost = 0 THEN 0 ELSE cte3.sum_standard_cost END
        ) * r.required_quantity
      ) AS mtl_cost_at_close,
      CASE WHEN cte1.standard_cost = 0 THEN 0 ELSE cte3.sum_standard_cost END AS unit_mtl_cost_at_close,
      (
        SELECT
          SUM(base_transaction_value)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.MTL_TRANSACTION_ACCOUNTS
          WHERE
            is_deleted_flg <> 'Y'
        ) AS a, (
          SELECT
            *
          FROM silver_bec_ods.mtl_material_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS t
        WHERE
          a.transaction_id = t.transaction_id
          AND t.transaction_source_type_id = 5
          AND t.transaction_source_id = e.wip_entity_id
          AND r.inventory_item_id = t.inventory_item_id
          AND A.COST_ELEMENT_ID = 2
          AND a.accounting_line_type = 7
      ) AS mtl_oh_transaction_value,
      (
        (
          CASE WHEN cte1.standard_cost = 0 THEN 0 ELSE cte4.sum_standard_cost END
        ) * r.required_quantity
      ) AS mtl_oh_cost_at_close,
      CASE WHEN cte1.standard_cost = 0 THEN 0 ELSE cte4.sum_standard_cost END AS unit_mtl_oh_cost_at_close,
      (
        SELECT
          SUM(base_transaction_value)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.MTL_TRANSACTION_ACCOUNTS
          WHERE
            is_deleted_flg <> 'Y'
        ) AS a, (
          SELECT
            *
          FROM silver_bec_ods.mtl_material_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS t
        WHERE
          a.transaction_id = t.transaction_id
          AND t.transaction_source_type_id = 5
          AND t.transaction_source_id = e.wip_entity_id
          AND r.inventory_item_id = t.inventory_item_id
          AND A.COST_ELEMENT_ID = 3
          AND a.accounting_line_type = 7
      ) AS res_transaction_value,
      (
        (
          CASE WHEN cte1.standard_cost = 0 THEN 0 ELSE cte5.sum_standard_cost END
        ) * r.required_quantity
      ) AS res_cost_at_close,
      CASE WHEN cte1.standard_cost = 0 THEN 0 ELSE cte5.sum_standard_cost END AS unit_res_cost_at_close,
      (
        SELECT
          SUM(base_transaction_value)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.MTL_TRANSACTION_ACCOUNTS
          WHERE
            is_deleted_flg <> 'Y'
        ) AS a, (
          SELECT
            *
          FROM silver_bec_ods.mtl_material_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS t
        WHERE
          a.transaction_id = t.transaction_id
          AND t.transaction_source_type_id = 5
          AND t.transaction_source_id = e.wip_entity_id
          AND r.inventory_item_id = t.inventory_item_id
          AND A.COST_ELEMENT_ID = 4
          AND a.accounting_line_type = 7
      ) AS osp_transaction_value,
      (
        (
          CASE WHEN cte1.standard_cost = 0 THEN 0 ELSE cte6.sum_standard_cost END
        ) * r.required_quantity
      ) AS osp_cost_at_close,
      CASE WHEN cte1.standard_cost = 0 THEN 0 ELSE cte6.sum_standard_cost END AS unit_osp_cost_at_close,
      (
        SELECT
          SUM(base_transaction_value)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.MTL_TRANSACTION_ACCOUNTS
          WHERE
            is_deleted_flg <> 'Y'
        ) AS a, (
          SELECT
            *
          FROM silver_bec_ods.mtl_material_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS t
        WHERE
          a.transaction_id = t.transaction_id
          AND t.transaction_source_type_id = 5
          AND t.transaction_source_id = e.wip_entity_id
          AND r.inventory_item_id = t.inventory_item_id
          AND A.COST_ELEMENT_ID = 5
          AND a.accounting_line_type = 7
      ) AS oh_transaction_value,
      (
        (
          CASE WHEN cte1.standard_cost = 0 THEN 0 ELSE cte7.sum_standard_cost END
        ) * r.required_quantity
      ) AS oh_cost_at_close,
      CASE WHEN cte1.standard_cost = 0 THEN 0 ELSE cte7.sum_standard_cost END AS unit_oh_cost_at_close,
      NULL AS cost_update_txn_value,
      NULL AS Mtl_variance,
      NULL AS mtl_oh_variance,
      NULL AS Res_variance,
      NULL AS osp_variance,
      NULL AS oh_variance,
      NULL AS net_value,
      d.organization_id AS organization_id1,
      NULL AS ALTERNATE_BOM,
      'Work Order Components' AS COMMENTS
    FROM we AS e
    INNER JOIN wdj AS d
      ON d.wip_entity_id = e.wip_entity_id
    INNER JOIN wro AS r
      ON d.wip_entity_id = r.wip_entity_id AND d.organization_id = r.organization_id
    LEFT OUTER JOIN cte1 AS cte1
      ON r.inventory_item_id = cte1.inventory_item_id
      AND r.organization_id = cte1.organization_id
      AND e.wip_entity_id = cte1.wip_entity_id
    LEFT OUTER JOIN cte2 AS cte2
      ON r.inventory_item_id = cte2.inventory_item_id
      AND r.organization_id = cte2.organization_id
      AND r.wip_entity_id = cte2.wip_entity_id
    LEFT OUTER JOIN cte3 AS cte3
      ON r.inventory_item_id = cte3.inventory_item_id
      AND r.organization_id = cte3.organization_id
      AND r.wip_entity_id = cte3.wip_entity_id
    LEFT OUTER JOIN cte4 AS cte4
      ON r.inventory_item_id = cte4.inventory_item_id
      AND r.organization_id = cte4.organization_id
      AND r.wip_entity_id = cte4.wip_entity_id
    LEFT OUTER JOIN cte5 AS cte5
      ON r.inventory_item_id = cte5.inventory_item_id
      AND r.organization_id = cte5.organization_id
      AND r.wip_entity_id = cte5.wip_entity_id
    LEFT OUTER JOIN cte6 AS cte6
      ON r.inventory_item_id = cte6.inventory_item_id
      AND r.organization_id = cte6.organization_id
      AND r.wip_entity_id = cte6.wip_entity_id
    LEFT OUTER JOIN cte7 AS cte7
      ON r.inventory_item_id = cte7.inventory_item_id
      AND r.organization_id = cte7.organization_id
      AND r.wip_entity_id = cte7.wip_entity_id
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wo_value_variance_union1_stg' AND batch_name = 'wip';