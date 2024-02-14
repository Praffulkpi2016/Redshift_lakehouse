/* Delete Records */
DROP table IF EXISTS gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG_INCR;
CREATE TABLE gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG_INCR AS
(
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY dw_load_id) AS rownumber
  FROM (
    /* ---------------------------------------------------------------------------------- */
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
        AND (
          kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_wo_value_variance_union6_stg' AND batch_name = 'wip'
          )
        )
    ), wor /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        organization_id,
        wip_entity_id,
        resource_id,
        department_id,
        operation_seq_num,
        resource_seq_num,
        uom_code,
        usage_rate_or_amount,
        applied_resource_units
      FROM (
        SELECT
          *
        FROM silver_bec_ods.WIP_OPERATION_RESOURCES
        WHERE
          is_deleted_flg <> 'Y'
      )
      WHERE
        1 = 1
        AND (
          kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_wo_value_variance_union6_stg' AND batch_name = 'wip'
          )
        )
    ), wdj /* ----------------------------------------------------------------------------------	*/ AS (
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
        AND (
          kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_wo_value_variance_union6_stg' AND batch_name = 'wip'
          )
        )
    ), we /* ---------------------------------------------------------------------------------- */ AS (
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
        AND (
          kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_wo_value_variance_union6_stg' AND batch_name = 'wip'
          )
        )
    ), csc /* ---------------------------------------------------------------------------------- */ AS (
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
        AND (
          kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_wo_value_variance_union6_stg' AND batch_name = 'wip'
          )
        )
    ), csc_max_id /* -----------------------------------------------------------------		*/ AS (
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
    ), csc_max_id1 /* ----------------------------------------------------------------- */ AS (
      SELECT
        MAX(c2.cost_update_id) AS max_cost_update_id1,
        c2.inventory_item_id,
        c2.organization_id /* ,d.wip_entity_id */
      FROM csc AS c2
      INNER JOIN wdj AS d
        ON COALESCE(c2.inventory_item_id, 0) = COALESCE(d.primary_item_id, 0)
        AND COALESCE(c2.organization_id, 0) = COALESCE(d.organization_id, 0)
        AND c2.standard_cost_revision_date <= COALESCE(d.date_closed, (
          CURRENT_TIMESTAMP()
        ))
      GROUP BY
        c2.inventory_item_id,
        c2.organization_id /* ,d.wip_entity_id */
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
    ), cec_max_date1 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        MAX(e2.creation_date) AS max_creation_date1,
        e2.inventory_item_id,
        e2.organization_id,
        d.wip_entity_id
      FROM cec AS e2
      INNER JOIN wdj AS d
        ON COALESCE(e2.inventory_item_id, 0) = COALESCE(d.primary_item_id, 0)
        AND COALESCE(e2.organization_id, 0) = COALESCE(d.organization_id, 0)
        AND e2.creation_date <= COALESCE(d.date_closed, (
          CURRENT_TIMESTAMP()
        ))
      GROUP BY
        e2.inventory_item_id,
        e2.organization_id,
        d.wip_entity_id
    ), bsb /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        b.ASSEMBLY_ITEM_ID,
        b.ORGANIZATION_ID,
        b.ALTERNATE_BOM_DESIGNATOR,
        b.LAST_UPDATE_DATE,
        b.BILL_SEQUENCE_ID
      FROM (
        SELECT
          *
        FROM silver_bec_ods.bom_structures_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS b
      WHERE
        b.obj_name IS NULL
        AND COALESCE(b.effectivity_control, 1) <= 3
        AND (
          b.kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_wo_value_variance_union6_stg' AND batch_name = 'wip'
          )
        )
    ), bcb /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        COMPONENT_ITEM_ID,
        LAST_UPDATE_DATE,
        COMPONENT_QUANTITY,
        INCLUDE_IN_COST_ROLLUP,
        BILL_SEQUENCE_ID,
        WIP_SUPPLY_TYPE
      FROM (
        SELECT
          *
        FROM silver_bec_ods.bom_components_b
        WHERE
          is_deleted_flg <> 'Y'
      )
      WHERE
        obj_name IS NULL
        AND overlapping_changes IS NULL
        AND (
          kca_seq_date > (
            SELECT
              (
                executebegints - prune_days
              )
            FROM bec_etl_ctrl.batch_dw_info
            WHERE
              dw_table_name = 'fact_wo_value_variance_union6_stg' AND batch_name = 'wip'
          )
        ) /*	AND include_in_cost_rollup = 1 */
    ), csc_max_id2 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        MAX(c2.cost_update_id) AS max_cost_update_id2,
        c2.inventory_item_id,
        c2.organization_id /* ,d.wip_entity_id */
      FROM csc AS c2
      INNER JOIN bsb AS b
        ON c2.organization_id = b.organization_id
      INNER JOIN bcb AS c
        ON b.bill_sequence_id = c.bill_sequence_id
        AND c2.inventory_item_id = c.component_item_id
      INNER JOIN wdj AS d
        ON b.assembly_item_id = d.primary_item_id AND b.organization_id = d.organization_id
      WHERE
        c2.standard_cost_revision_date <= COALESCE(d.date_closed, (
          CURRENT_TIMESTAMP()
        ))
      GROUP BY
        c2.inventory_item_id,
        c2.organization_id /* ,d.wip_entity_id */
    ), cte15 /* ----------------------------------------------------------------- */ AS (
      SELECT
        c1.inventory_item_id,
        c1.organization_id,
        c1.cost_update_id,
        d.wip_entity_id,
        c1.standard_cost
      FROM csc AS c1
      INNER JOIN bsb AS b
        ON c1.organization_id = b.organization_id
      INNER JOIN bcb AS c
        ON b.bill_sequence_id = c.bill_sequence_id
        AND c1.inventory_item_id = c.component_item_id
      INNER JOIN wdj AS d
        ON b.assembly_item_id = d.primary_item_id AND b.organization_id = d.organization_id
      WHERE
        c1.standard_cost_revision_date <= COALESCE(d.date_closed, (
          CURRENT_TIMESTAMP()
        ))
        AND c1.cost_update_id IN (
          SELECT
            max_cost_update_id2
          FROM csc_max_id2
          WHERE
            inventory_item_id = c1.inventory_item_id AND organization_id = c1.organization_id
        ) /*	and wip_entity_id = d.wip_entity_id */
      GROUP BY
        c1.inventory_item_id,
        c1.organization_id,
        c1.cost_update_id,
        d.wip_entity_id,
        c1.standard_cost
    ), cec_max_date2 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        MAX(e2.creation_date) AS max_creation_date2,
        e2.inventory_item_id,
        e2.organization_id,
        d.wip_entity_id
      FROM cec AS e2
      INNER JOIN bsb AS b
        ON e2.organization_id = b.organization_id
      INNER JOIN bcb AS c
        ON b.bill_sequence_id = c.bill_sequence_id
        AND e2.inventory_item_id = c.component_item_id
      INNER JOIN wdj AS d
        ON b.assembly_item_id = d.primary_item_id AND b.organization_id = d.organization_id
      WHERE
        e2.creation_date <= COALESCE(d.date_closed, (
          CURRENT_TIMESTAMP()
        ))
      GROUP BY
        e2.inventory_item_id,
        e2.organization_id,
        d.wip_entity_id
    ), cte_16 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        e1.standard_cost,
        e1.inventory_item_id,
        e1.organization_id,
        d.wip_entity_id
      FROM (
        SELECT
          inventory_item_id,
          organization_id,
          cost_element_id,
          creation_date,
          standard_cost
        FROM cec
      ) AS e1
      INNER JOIN bsb AS b
        ON e1.organization_id = b.organization_id
      INNER JOIN bcb AS c
        ON b.bill_sequence_id = c.bill_sequence_id
        AND e1.inventory_item_id = c.component_item_id
      INNER JOIN wdj AS d
        ON b.assembly_item_id = d.primary_item_id AND b.organization_id = d.organization_id
      WHERE
        e1.creation_date IN (
          SELECT
            max_creation_date2
          FROM cec_max_date2
          WHERE
            inventory_item_id = e1.inventory_item_id
            AND organization_id = e1.organization_id
            AND wip_entity_id = d.wip_entity_id
        )
      GROUP BY
        e1.standard_cost,
        e1.inventory_item_id,
        e1.organization_id,
        d.wip_entity_id
    ), cte16 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        SUM(standard_cost) AS sum_standard_cost,
        inventory_item_id,
        organization_id,
        wip_entity_id
      FROM cte_16
      GROUP BY
        inventory_item_id,
        organization_id,
        wip_entity_id
    ), cte_17 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        e1.standard_cost,
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
      INNER JOIN bsb AS b
        ON e1.organization_id = b.organization_id
      INNER JOIN bcb AS c
        ON b.bill_sequence_id = c.bill_sequence_id
        AND e1.inventory_item_id = c.component_item_id
      INNER JOIN wdj AS d
        ON b.assembly_item_id = d.primary_item_id AND b.organization_id = d.organization_id
      WHERE
        e1.creation_date IN (
          SELECT
            max_creation_date2
          FROM cec_max_date2
          WHERE
            inventory_item_id = e1.inventory_item_id
            AND organization_id = e1.organization_id
            AND wip_entity_id = d.wip_entity_id
        )
      GROUP BY
        e1.standard_cost,
        e1.inventory_item_id,
        e1.organization_id,
        d.wip_entity_id
    ), cte17 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        SUM(standard_cost) AS sum_standard_cost,
        inventory_item_id,
        organization_id,
        wip_entity_id
      FROM cte_17
      GROUP BY
        inventory_item_id,
        organization_id,
        wip_entity_id
    ), cte_18 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        e1.standard_cost,
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
      INNER JOIN bsb AS b
        ON e1.organization_id = b.organization_id
      INNER JOIN bcb AS c
        ON b.bill_sequence_id = c.bill_sequence_id
        AND e1.inventory_item_id = c.component_item_id
      INNER JOIN wdj AS d
        ON b.assembly_item_id = d.primary_item_id AND b.organization_id = d.organization_id
      WHERE
        e1.creation_date IN (
          SELECT
            max_creation_date2
          FROM cec_max_date2
          WHERE
            inventory_item_id = e1.inventory_item_id
            AND organization_id = e1.organization_id
            AND wip_entity_id = d.wip_entity_id
        )
      GROUP BY
        e1.standard_cost,
        e1.inventory_item_id,
        e1.organization_id,
        d.wip_entity_id
    ), cte18 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        SUM(standard_cost) AS sum_standard_cost,
        inventory_item_id,
        organization_id,
        wip_entity_id
      FROM cte_18
      GROUP BY
        inventory_item_id,
        organization_id,
        wip_entity_id
    ), cte_19 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        e1.standard_cost,
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
      INNER JOIN bsb AS b
        ON e1.organization_id = b.organization_id
      INNER JOIN bcb AS c
        ON b.bill_sequence_id = c.bill_sequence_id
        AND e1.inventory_item_id = c.component_item_id
      INNER JOIN wdj AS d
        ON b.assembly_item_id = d.primary_item_id AND b.organization_id = d.organization_id
      WHERE
        e1.creation_date IN (
          SELECT
            max_creation_date2
          FROM cec_max_date2
          WHERE
            inventory_item_id = e1.inventory_item_id
            AND organization_id = e1.organization_id
            AND wip_entity_id = d.wip_entity_id
        )
      GROUP BY
        e1.standard_cost,
        e1.inventory_item_id,
        e1.organization_id,
        d.wip_entity_id
    ), cte19 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        SUM(standard_cost) AS sum_standard_cost,
        inventory_item_id,
        organization_id,
        wip_entity_id
      FROM cte_19
      GROUP BY
        inventory_item_id,
        organization_id,
        wip_entity_id
    ), cte_20 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        e1.standard_cost,
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
      INNER JOIN bsb AS b
        ON e1.organization_id = b.organization_id
      INNER JOIN bcb AS c
        ON b.bill_sequence_id = c.bill_sequence_id
        AND e1.inventory_item_id = c.component_item_id
      INNER JOIN wdj AS d
        ON b.assembly_item_id = d.primary_item_id AND b.organization_id = d.organization_id
      WHERE
        e1.creation_date IN (
          SELECT
            max_creation_date2
          FROM cec_max_date2
          WHERE
            inventory_item_id = e1.inventory_item_id
            AND organization_id = e1.organization_id
            AND wip_entity_id = d.wip_entity_id
        )
      GROUP BY
        e1.standard_cost,
        e1.inventory_item_id,
        e1.organization_id,
        d.wip_entity_id
    ), cte20 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        SUM(standard_cost) AS sum_standard_cost,
        inventory_item_id,
        organization_id,
        wip_entity_id
      FROM cte_20
      GROUP BY
        inventory_item_id,
        organization_id,
        wip_entity_id
    ), cte_21 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        e1.standard_cost,
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
      INNER JOIN bsb AS b
        ON e1.organization_id = b.organization_id
      INNER JOIN bcb AS c
        ON b.bill_sequence_id = c.bill_sequence_id
        AND e1.inventory_item_id = c.component_item_id
      INNER JOIN wdj AS d
        ON b.assembly_item_id = d.primary_item_id AND b.organization_id = d.organization_id
      WHERE
        e1.creation_date IN (
          SELECT
            max_creation_date2
          FROM cec_max_date2
          WHERE
            inventory_item_id = e1.inventory_item_id
            AND organization_id = e1.organization_id
            AND wip_entity_id = d.wip_entity_id
        )
      GROUP BY
        e1.standard_cost,
        e1.inventory_item_id,
        e1.organization_id,
        d.wip_entity_id
    ), cte21 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        SUM(standard_cost) AS sum_standard_cost,
        inventory_item_id,
        organization_id,
        wip_entity_id
      FROM cte_21
      GROUP BY
        inventory_item_id,
        organization_id,
        wip_entity_id
    ), cte22 /* ---------------------------------------------------------------------------------- */ AS (
      SELECT
        e.organization_id,
        e.primary_item_id,
        c.component_item_id AS inventory_item_id,
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
            organization_id = b.organization_id AND inventory_item_id = c.component_item_id
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
            organization_id = b.organization_id AND inventory_item_id = c.component_item_id
        ) AS item_description,
        NULL AS operation_seq_num,
        c.component_quantity AS quantity_per_assembly,
        NULL AS required_quantity,
        NULL AS quantity_issued,
        CAST(CASE
          WHEN CAST(CAST(c.wip_supply_type AS INT) AS STRING) = 1
          THEN 'Push'
          WHEN CAST(CAST(c.wip_supply_type AS INT) AS STRING) = 2
          THEN 'Assembly Pull'
          WHEN CAST(CAST(c.wip_supply_type AS INT) AS STRING) = 3
          THEN 'Operation Pull'
          WHEN CAST(CAST(c.wip_supply_type AS INT) AS STRING) = 4
          THEN 'Bulk'
          WHEN CAST(CAST(c.wip_supply_type AS INT) AS STRING) = 5
          THEN 'Vendor'
          WHEN CAST(CAST(c.wip_supply_type AS INT) AS STRING) = 6
          THEN 'Phantom'
          WHEN CAST(CAST(c.wip_supply_type AS INT) AS STRING) = 7
          THEN 'Based on Bill'
          ELSE CAST(CAST(c.wip_supply_type AS INT) AS STRING)
        END AS STRING) AS WIP_Supply_Type,
        NULL AS department_code,
        NULL AS res_op_seq_num,
        NULL AS resource_seq_num,
        NULL AS resource_code,
        NULL AS res_uom_code,
        NULL AS Res_usage_rate,
        NULL AS applied_resource_units,
        NULL AS item_transaction_value,
        (
          (
            CASE WHEN cte15.standard_cost = 0 THEN 0 ELSE cte16.sum_standard_cost END
          ) * c.component_quantity
        ) AS item_cost_at_close,
        CASE WHEN cte15.standard_cost = 0 THEN 0 ELSE cte16.sum_standard_cost END AS unit_item_cost_at_close,
        NULL AS mtl_transaction_value,
        (
          (
            CASE WHEN cte15.standard_cost = 0 THEN 0 ELSE cte17.sum_standard_cost END
          ) * c.component_quantity
        ) AS mtl_cost_at_close,
        CASE WHEN cte15.standard_cost = 0 THEN 0 ELSE cte17.sum_standard_cost END AS unit_mtl_cost_at_close,
        NULL AS mtl_oh_transaction_value,
        (
          (
            CASE WHEN cte15.standard_cost = 0 THEN 0 ELSE cte18.sum_standard_cost END
          ) * c.component_quantity
        ) AS mtl_oh_cost_at_close,
        CASE WHEN cte15.standard_cost = 0 THEN 0 ELSE cte18.sum_standard_cost END AS unit_mtl_oh_cost_at_close,
        NULL AS res_transaction_value,
        (
          (
            CASE WHEN cte15.standard_cost = 0 THEN 0 ELSE cte19.sum_standard_cost END
          ) * c.component_quantity
        ) AS res_cost_at_close,
        CASE WHEN cte15.standard_cost = 0 THEN 0 ELSE cte19.sum_standard_cost END AS unit_res_cost_at_close,
        NULL AS osp_transaction_value,
        (
          (
            CASE WHEN cte15.standard_cost = 0 THEN 0 ELSE cte20.sum_standard_cost END
          ) * c.component_quantity
        ) AS osp_cost_at_close,
        CASE WHEN cte15.standard_cost = 0 THEN 0 ELSE cte20.sum_standard_cost END AS unit_osp_cost_at_close,
        NULL AS oh_transaction_value,
        (
          (
            CASE WHEN cte15.standard_cost = 0 THEN 0 ELSE cte21.sum_standard_cost END
          ) * c.component_quantity
        ) AS oh_cost_at_close,
        CASE WHEN cte15.standard_cost = 0 THEN 0 ELSE cte21.sum_standard_cost END AS unit_oh_cost_at_close,
        NULL AS cost_update_txn_value,
        NULL AS mtl_variance,
        NULL AS mtl_oh_variance,
        NULL AS res_variance,
        NULL AS osp_variance,
        NULL AS oh_variance,
        NULL AS net_value,
        d.organization_id AS organization_id1,
        b.alternate_bom_designator AS ALTERNATE_BOM,
        'BOM Components not in Work Order' AS COMMENTS /* ,
        ROW_NUMBER () OVER (PARTITION BY 
        e.organization_id,e.primary_item_id,c.component_item_id,e.wip_entity_id
        ORDER BY e.organization_id,e.primary_item_id) AS RN	*/
      /*                       e1.creation_date */
      FROM bsb AS b
      INNER JOIN bcb AS c
        ON b.bill_sequence_id = c.bill_sequence_id
      INNER JOIN wdj AS d
        ON b.assembly_item_id = d.primary_item_id AND b.organization_id = d.organization_id
      INNER JOIN we AS e
        ON d.wip_entity_id = e.wip_entity_id
        AND c.include_in_cost_rollup = 1
        AND (
          (
            SELECT
              COUNT(1)
            FROM (
              SELECT
                *
              FROM silver_bec_ods.WIP_REQUIREMENT_OPERATIONS
              WHERE
                is_deleted_flg <> 'Y'
            )
            WHERE
              wip_entity_id = e.wip_entity_id AND inventory_item_id = c.component_item_id
          ) = 0
        )
      LEFT OUTER JOIN cte15
        ON e.wip_entity_id = cte15.wip_entity_id
        AND c.component_item_id = cte15.inventory_item_id
        AND b.organization_id = cte15.organization_id
      LEFT OUTER JOIN cte16
        ON e.wip_entity_id = cte16.wip_entity_id
        AND c.component_item_id = cte16.inventory_item_id
        AND b.organization_id = cte16.organization_id
      LEFT OUTER JOIN cte17
        ON e.wip_entity_id = cte17.wip_entity_id
        AND c.component_item_id = cte17.inventory_item_id
        AND b.organization_id = cte17.organization_id
      LEFT OUTER JOIN cte18
        ON e.wip_entity_id = cte18.wip_entity_id
        AND c.component_item_id = cte18.inventory_item_id
        AND b.organization_id = cte18.organization_id
      LEFT OUTER JOIN cte19
        ON e.wip_entity_id = cte19.wip_entity_id
        AND c.component_item_id = cte19.inventory_item_id
        AND b.organization_id = cte19.organization_id
      LEFT OUTER JOIN cte20
        ON e.wip_entity_id = cte20.wip_entity_id
        AND c.component_item_id = cte20.inventory_item_id
        AND b.organization_id = cte20.organization_id
      LEFT OUTER JOIN cte21
        ON e.wip_entity_id = cte21.wip_entity_id
        AND c.component_item_id = cte21.inventory_item_id
        AND b.organization_id = cte21.organization_id
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
      CAST(required_quantity AS INT) AS required_quantity,
      CAST(quantity_issued AS INT) AS quantity_issued,
      WIP_Supply_Type,
      CAST(department_code AS STRING) AS department_code,
      CAST(res_op_seq_num AS INT) AS res_op_seq_num,
      CAST(resource_seq_num AS INT) AS resource_seq_num,
      CAST(resource_code AS STRING) AS resource_code,
      CAST(res_uom_code AS STRING) AS res_uom_code,
      CAST(Res_usage_rate AS INT) AS Res_usage_rate,
      CAST(applied_resource_units AS INT) AS applied_resource_units,
      CAST(item_transaction_value AS INT) AS item_transaction_value,
      item_cost_at_close,
      unit_item_cost_at_close,
      CAST(mtl_transaction_value AS INT) AS mtl_transaction_value,
      mtl_cost_at_close,
      unit_mtl_cost_at_close,
      CAST(mtl_oh_transaction_value AS INT) AS mtl_oh_transaction_value,
      mtl_oh_cost_at_close,
      unit_mtl_oh_cost_at_close,
      CAST(res_transaction_value AS INT) AS res_transaction_value,
      res_cost_at_close,
      unit_res_cost_at_close,
      CAST(osp_transaction_value AS INT) AS osp_transaction_value,
      osp_cost_at_close,
      unit_osp_cost_at_close,
      CAST(oh_transaction_value AS INT) AS oh_transaction_value,
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
        cte22.organization_id,
        cte22.primary_item_id,
        cte22.inventory_item_id,
        cte22.wip_entity_id,
        cte22.work_order,
        cte22.wo_description,
        cte22.assembly_item,
        cte22.ASSEMBLY_DESCRIPTION,
        cte22.status_type,
        cte22.start_quantity,
        cte22.quantity_completed,
        cte22.quantity_scrapped,
        cte22.date_released,
        cte22.date_completed,
        cte22.date_closed,
        cte22.JOB_START_DATE,
        cte22.class_code,
        cte22.item_number,
        cte22.item_description,
        cte22.operation_seq_num,
        cte22.quantity_per_assembly,
        cte22.required_quantity,
        cte22.quantity_issued,
        cte22.WIP_Supply_Type,
        cte22.department_code,
        cte22.res_op_seq_num,
        cte22.resource_seq_num,
        cte22.resource_code,
        cte22.res_uom_code,
        cte22.Res_usage_rate,
        cte22.applied_resource_units,
        cte22.item_transaction_value,
        cte22.item_cost_at_close,
        cte22.unit_item_cost_at_close,
        cte22.mtl_transaction_value,
        cte22.mtl_cost_at_close,
        cte22.unit_mtl_cost_at_close,
        cte22.mtl_oh_transaction_value,
        cte22.mtl_oh_cost_at_close,
        cte22.unit_mtl_oh_cost_at_close,
        cte22.res_transaction_value,
        cte22.res_cost_at_close,
        cte22.unit_res_cost_at_close,
        cte22.osp_transaction_value,
        cte22.osp_cost_at_close,
        cte22.unit_osp_cost_at_close,
        cte22.oh_transaction_value,
        cte22.oh_cost_at_close,
        cte22.unit_oh_cost_at_close,
        cte22.cost_update_txn_value,
        cte22.mtl_variance,
        cte22.mtl_oh_variance,
        cte22.res_variance,
        cte22.osp_variance,
        cte22.oh_variance,
        cte22.net_value,
        cte22.organization_id1,
        cte22.ALTERNATE_BOM,
        cte22.COMMENTS
      FROM cte22 AS cte22
      INNER JOIN cec AS e1
        ON e1.inventory_item_id = cte22.inventory_item_id
        AND e1.organization_id = cte22.organization_id1
      INNER JOIN cec_max_date2 AS e2
        ON e2.inventory_item_id = cte22.inventory_item_id
        AND e2.organization_id = cte22.organization_id1
        AND e2.wip_entity_id = cte22.wip_entity_id
      WHERE
        e1.creation_date = e2.max_creation_date2
    )
  )
);
/* ------------------------------------------------------------------ */ /* ------------------------------------------------------------------ */
DELETE FROM gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG
WHERE
  (dw_load_id, rownumber) IN (
    SELECT
      ods.dw_load_id,
      ods.rownumber
    FROM gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG_INCR AS ods, gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG AS dw
    WHERE
      1 = 1 AND dw.dw_load_id = ods.dw_load_id AND dw.rownumber = ods.rownumber
  );
/* ------------------------------------------------------------------------- */ /* -------------------------------------------------------------------------- */
INSERT INTO gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG
SELECT
  *
FROM gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION6_STG_INCR;
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wo_value_variance_union6_stg' AND batch_name = 'wip';