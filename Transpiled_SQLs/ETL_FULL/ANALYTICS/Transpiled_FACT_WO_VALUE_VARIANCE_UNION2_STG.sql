DROP table IF EXISTS gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION2_STG;
CREATE TABLE gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION2_STG AS
/* ---------------------------------------------------------------------------------- */
/* ---------------------------------------------------------------------------------- */
WITH mta AS (
  SELECT
    transaction_id,
    base_transaction_value,
    accounting_line_type,
    cost_element_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.MTL_TRANSACTION_ACCOUNTS
    WHERE
      is_deleted_flg <> 'Y'
  )
  WHERE
    accounting_line_type = 7
), mmt /* ----------------------------------------------------------------------------------		*/ AS (
  SELECT
    transaction_id,
    transaction_source_type_id,
    transaction_source_id,
    inventory_item_id
  FROM (
    SELECT
      *
    FROM silver_bec_ods.mtl_material_transactions
    WHERE
      is_deleted_flg <> 'Y'
  )
  WHERE
    transaction_source_type_id = 5
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
), cte8 /* ---------------------------------------------------------------------------------- */ AS (
  SELECT
    c1.inventory_item_id,
    c1.organization_id,
    c1.cost_update_id,
    d.wip_entity_id,
    c1.standard_cost
  FROM csc AS c1
  INNER JOIN wdj AS d
    ON c1.inventory_item_id = d.primary_item_id
    AND c1.organization_id = d.organization_id
    AND c1.standard_cost_revision_date <= COALESCE(d.date_closed, (
      CURRENT_TIMESTAMP()
    ))
    AND c1.cost_update_id IN (
      SELECT
        max_cost_update_id1
      FROM csc_max_id1
      WHERE
        inventory_item_id = c1.inventory_item_id AND organization_id = c1.organization_id
    ) /* and wip_entity_id = d.wip_entity_id */
), cte9 /* ---------------------------------------------------------------------------------- */ AS (
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
  INNER JOIN wdj AS d
    ON e1.inventory_item_id = d.primary_item_id
    AND e1.organization_id = d.organization_id
    AND e1.creation_date IN (
      SELECT
        max_creation_date1
      FROM cec_max_date1
      WHERE
        inventory_item_id = e1.inventory_item_id
        AND organization_id = e1.organization_id
        AND wip_entity_id = d.wip_entity_id
    )
  GROUP BY
    e1.inventory_item_id,
    e1.organization_id,
    d.wip_entity_id
), cte10 /* ---------------------------------------------------------------------------------- */ AS (
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
  INNER JOIN wdj AS d
    ON e1.inventory_item_id = d.primary_item_id
    AND e1.organization_id = d.organization_id
    AND e1.creation_date IN (
      SELECT
        max_creation_date1
      FROM cec_max_date1
      WHERE
        inventory_item_id = e1.inventory_item_id
        AND organization_id = e1.organization_id
        AND wip_entity_id = d.wip_entity_id
    )
  GROUP BY
    e1.inventory_item_id,
    e1.organization_id,
    d.wip_entity_id
), cte11 /* ---------------------------------------------------------------------------------- */ AS (
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
  INNER JOIN wdj AS d
    ON e1.inventory_item_id = d.primary_item_id
    AND e1.organization_id = d.organization_id
    AND e1.creation_date IN (
      SELECT
        max_creation_date1
      FROM cec_max_date1
      WHERE
        inventory_item_id = e1.inventory_item_id
        AND organization_id = e1.organization_id
        AND wip_entity_id = d.wip_entity_id
    )
  GROUP BY
    e1.inventory_item_id,
    e1.organization_id,
    d.wip_entity_id
), cte12 /* ---------------------------------------------------------------------------------- */ AS (
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
  INNER JOIN wdj AS d
    ON e1.inventory_item_id = d.primary_item_id
    AND e1.organization_id = d.organization_id
    AND e1.creation_date IN (
      SELECT
        max_creation_date1
      FROM cec_max_date1
      WHERE
        inventory_item_id = e1.inventory_item_id
        AND organization_id = e1.organization_id
        AND wip_entity_id = d.wip_entity_id
    )
  GROUP BY
    e1.inventory_item_id,
    e1.organization_id,
    d.wip_entity_id
), cte13 /* ---------------------------------------------------------------------------------- */ AS (
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
  INNER JOIN wdj AS d
    ON e1.inventory_item_id = d.primary_item_id
    AND e1.organization_id = d.organization_id
    AND e1.creation_date IN (
      SELECT
        max_creation_date1
      FROM cec_max_date1
      WHERE
        inventory_item_id = e1.inventory_item_id
        AND organization_id = e1.organization_id
        AND wip_entity_id = d.wip_entity_id
    )
  GROUP BY
    e1.inventory_item_id,
    e1.organization_id,
    d.wip_entity_id
), cte14 /* ---------------------------------------------------------------------------------- */ AS (
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
  INNER JOIN wdj AS d
    ON e1.inventory_item_id = d.primary_item_id
    AND e1.organization_id = d.organization_id
    AND e1.creation_date IN (
      SELECT
        max_creation_date1
      FROM cec_max_date1
      WHERE
        inventory_item_id = e1.inventory_item_id
        AND organization_id = e1.organization_id
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
  CAST(inventory_item_id AS INT) AS inventory_item_id,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || COALESCE(CAST(inventory_item_id AS INT), 0) AS inventory_item_id_key,
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
  CAST(quantity_per_assembly AS INT) AS quantity_per_assembly,
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
  ) || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(CAST(inventory_item_id AS INT), 0) || '-' || COALESCE(wip_entity_id, 0) || '-' || COALESCE(CAST(operation_seq_num AS INT), 0) || '-' || COALESCE(CAST(res_op_seq_num AS INT), 0) || '-' || COALESCE(CAST(resource_seq_num AS INT), 0) || '-' || COALESCE(WIP_Supply_Type, 'NA') || '-' || COALESCE(ALTERNATE_BOM, 'NA') || '-' || COALESCE(comments, 'NA') AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    e.organization_id,
    e.primary_item_id,
    NULL AS inventory_item_id,
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
        organization_id = d.organization_id AND inventory_item_id = d.primary_item_id
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
        organization_id = d.organization_id AND inventory_item_id = d.primary_item_id
    ) AS item_description,
    NULL AS operation_seq_num,
    NULL AS quantity_per_assembly,
    NULL AS required_quantity,
    NULL AS quantity_issued,
    NULL AS WIP_Supply_Type,
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
      FROM mta AS a, mmt AS t
      WHERE
        a.transaction_id = t.transaction_id
        AND t.transaction_source_id = e.wip_entity_id
        AND t.inventory_item_id = d.primary_item_id
    ) AS item_transaction_value, /* y.transaction_type_name, */ /* AND A.COST_ELEMENT_ID = 1 */
    (
      (
        CASE WHEN cte8.standard_cost = 0 THEN 0 ELSE cte9.sum_standard_cost END
      ) * d.quantity_completed
    ) AS item_cost_at_close,
    CASE WHEN cte8.standard_cost = 0 THEN 0 ELSE cte9.sum_standard_cost END AS unit_item_cost_at_close,
    (
      SELECT
        SUM(base_transaction_value)
      FROM mta AS a, mmt AS t
      WHERE
        a.transaction_id = t.transaction_id
        AND t.transaction_source_id = e.wip_entity_id
        AND t.inventory_item_id = d.primary_item_id
        AND A.COST_ELEMENT_ID = 1
    ) AS mtl_transaction_value,
    (
      (
        CASE WHEN cte8.standard_cost = 0 THEN 0 ELSE cte10.sum_standard_cost END
      ) * d.quantity_completed
    ) AS mtl_cost_at_close,
    CASE WHEN cte8.standard_cost = 0 THEN 0 ELSE cte10.sum_standard_cost END AS unit_mtl_cost_at_close,
    (
      SELECT
        SUM(base_transaction_value)
      FROM mta AS a, mmt AS t
      WHERE
        a.transaction_id = t.transaction_id
        AND t.transaction_source_id = e.wip_entity_id
        AND t.inventory_item_id = d.primary_item_id
        AND A.COST_ELEMENT_ID = 2
    ) AS mtl_oh_transaction_value,
    (
      (
        CASE WHEN cte8.standard_cost = 0 THEN 0 ELSE cte11.sum_standard_cost END
      ) * d.quantity_completed
    ) AS mtl_oh_cost_at_close,
    CASE WHEN cte8.standard_cost = 0 THEN 0 ELSE cte11.sum_standard_cost END AS unit_mtl_oh_cost_at_close,
    (
      SELECT
        SUM(base_transaction_value)
      FROM mta AS a, mmt AS t
      WHERE
        a.transaction_id = t.transaction_id
        AND t.transaction_source_id = e.wip_entity_id
        AND t.inventory_item_id = d.primary_item_id
        AND A.COST_ELEMENT_ID = 3
    ) AS res_transaction_value,
    (
      (
        CASE WHEN cte8.standard_cost = 0 THEN 0 ELSE cte12.sum_standard_cost END
      ) * d.quantity_completed
    ) AS res_cost_at_close,
    CASE WHEN cte8.standard_cost = 0 THEN 0 ELSE cte12.sum_standard_cost END AS unit_res_cost_at_close,
    (
      SELECT
        SUM(base_transaction_value)
      FROM mta AS a, mmt AS t
      WHERE
        a.transaction_id = t.transaction_id
        AND t.transaction_source_id = e.wip_entity_id
        AND t.inventory_item_id = d.primary_item_id
        AND A.COST_ELEMENT_ID = 4
    ) AS osp_transaction_value,
    (
      (
        CASE WHEN cte8.standard_cost = 0 THEN 0 ELSE cte13.sum_standard_cost END
      ) * d.quantity_completed
    ) AS osp_cost_at_close,
    CASE WHEN cte8.standard_cost = 0 THEN 0 ELSE cte13.sum_standard_cost END AS unit_osp_cost_at_close,
    (
      SELECT
        SUM(base_transaction_value)
      FROM mta AS a, mmt AS t
      WHERE
        a.transaction_id = t.transaction_id
        AND t.transaction_source_id = e.wip_entity_id
        AND t.inventory_item_id = d.primary_item_id
        AND A.COST_ELEMENT_ID = 5
    ) AS oh_transaction_value,
    (
      (
        CASE WHEN cte8.standard_cost = 0 THEN 0 ELSE cte14.sum_standard_cost END
      ) * d.quantity_completed
    ) AS oh_cost_at_close,
    CASE WHEN cte8.standard_cost = 0 THEN 0 ELSE cte14.sum_standard_cost END AS unit_oh_cost_at_close,
    NULL AS cost_update_txn_value,
    NULL AS Mtl_variance,
    NULL AS mtl_oh_variance,
    NULL AS Res_variance,
    NULL AS osp_variance,
    NULL AS oh_variance,
    NULL AS net_value,
    d.organization_id AS organization_id1,
    NULL AS ALTERNATE_BOM,
    'Work Order Assembly' AS COMMENTS
  FROM we AS e
  INNER JOIN wdj AS d
    ON d.wip_entity_id = e.wip_entity_id
  LEFT OUTER JOIN cte8
    ON e.primary_item_id = cte8.inventory_item_id
    AND e.organization_id = cte8.organization_id
    AND e.wip_entity_id = cte8.wip_entity_id
  LEFT OUTER JOIN cte9
    ON e.organization_id = cte9.organization_id
    AND e.primary_item_id = cte9.inventory_item_id
    AND e.wip_entity_id = cte9.wip_entity_id
  LEFT OUTER JOIN cte10
    ON d.organization_id = cte10.organization_id
    AND d.primary_item_id = cte10.inventory_item_id
    AND e.wip_entity_id = cte10.wip_entity_id
  LEFT OUTER JOIN cte11
    ON d.organization_id = cte11.organization_id
    AND d.primary_item_id = cte11.inventory_item_id
    AND e.wip_entity_id = cte11.wip_entity_id
  LEFT OUTER JOIN cte12
    ON d.organization_id = cte12.organization_id
    AND d.primary_item_id = cte12.inventory_item_id
    AND e.wip_entity_id = cte12.wip_entity_id
  LEFT OUTER JOIN cte13
    ON d.organization_id = cte13.organization_id
    AND d.primary_item_id = cte13.inventory_item_id
    AND e.wip_entity_id = cte13.wip_entity_id
  LEFT OUTER JOIN cte14
    ON d.organization_id = cte14.organization_id
    AND d.primary_item_id = cte14.inventory_item_id
    AND e.wip_entity_id = cte14.wip_entity_id
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wo_value_variance_union2_stg' AND batch_name = 'wip';