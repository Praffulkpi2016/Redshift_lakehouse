DROP table IF EXISTS gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION3_STG;
CREATE TABLE gold_bec_dwh.FACT_WO_VALUE_VARIANCE_UNION3_STG AS
(
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
    CAST(inventory_item_id AS INT),
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
    CAST(item_number AS STRING) AS item_number,
    item_description,
    CAST(operation_seq_num AS INT) AS operation_seq_num,
    CAST(quantity_per_assembly AS INT) AS quantity_per_assembly,
    CAST(required_quantity AS INT) AS required_quantity,
    CAST(quantity_issued AS INT) AS quantity_issued,
    WIP_Supply_Type,
    department_code,
    CAST(res_op_seq_num AS INT) AS res_op_seq_num,
    CAST(resource_seq_num AS INT) AS resource_seq_num,
    resource_code,
    res_uom_code,
    Res_usage_rate,
    applied_resource_units,
    item_transaction_value,
    CAST(item_cost_at_close AS INT) AS item_cost_at_close,
    CAST(unit_item_cost_at_close AS INT) AS unit_item_cost_at_close,
    mtl_transaction_value,
    CAST(mtl_cost_at_close AS INT) AS mtl_cost_at_close,
    CAST(unit_mtl_cost_at_close AS INT) AS unit_mtl_cost_at_close,
    mtl_oh_transaction_value,
    CAST(mtl_oh_cost_at_close AS INT) AS mtl_oh_cost_at_close,
    CAST(unit_mtl_oh_cost_at_close AS INT) AS unit_mtl_oh_cost_at_close,
    res_transaction_value,
    CAST(res_cost_at_close AS INT) AS res_cost_at_close,
    CAST(unit_res_cost_at_close AS INT) AS unit_res_cost_at_close,
    osp_transaction_value,
    CAST(osp_cost_at_close AS INT) AS osp_cost_at_close,
    CAST(unit_osp_cost_at_close AS INT) AS unit_osp_cost_at_close,
    oh_transaction_value,
    CAST(oh_cost_at_close AS INT) AS oh_cost_at_close,
    CAST(unit_oh_cost_at_close AS INT) AS unit_oh_cost_at_close,
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
      NULL AS item_number,
      NULL AS item_description,
      NULL AS operation_seq_num,
      NULL AS quantity_per_assembly,
      NULL AS required_quantity,
      NULL AS quantity_issued,
      NULL AS WIP_Supply_Type,
      (
        SELECT
          department_code
        FROM (
          SELECT
            *
          FROM silver_bec_ods.bom_departments
          WHERE
            is_deleted_flg <> 'Y'
        ) AS bd
        WHERE
          r.organization_id = bd.organization_id AND r.department_id = bd.department_id
      ) AS department_code,
      r.operation_seq_num AS res_op_seq_num,
      r.resource_seq_num AS resource_seq_num,
      (
        SELECT
          resource_code
        FROM (
          SELECT
            *
          FROM silver_bec_ods.bom_resources
          WHERE
            is_deleted_flg <> 'Y'
        ) AS br
        WHERE
          r.organization_id = br.organization_id AND r.resource_id = br.resource_id
      ) AS resource_code,
      r.uom_code AS res_uom_code,
      r.usage_rate_or_amount AS Res_usage_rate,
      r.applied_resource_units AS applied_resource_units,
      (
        SELECT
          SUM(base_transaction_value)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.WIP_TRANSACTION_ACCOUNTS
          WHERE
            is_deleted_flg <> 'Y'
        ) AS a, (
          SELECT
            *
          FROM silver_bec_ods.wip_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS t
        WHERE
          a.transaction_id = t.transaction_id
          AND t.wip_entity_id = e.wip_entity_id
          AND t.resource_id = r.resource_id
          AND a.accounting_line_type = 7
      ) AS item_transaction_value,
      NULL AS item_cost_at_close,
      NULL AS unit_item_cost_at_close,
      (
        SELECT
          SUM(base_transaction_value)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.WIP_TRANSACTION_ACCOUNTS
          WHERE
            is_deleted_flg <> 'Y'
        ) AS a, (
          SELECT
            *
          FROM silver_bec_ods.wip_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS t
        WHERE
          a.transaction_id = t.transaction_id
          AND t.wip_entity_id = e.wip_entity_id
          AND t.resource_id = r.resource_id
          AND A.COST_ELEMENT_ID = 1
          AND a.accounting_line_type = 7
      ) AS mtl_transaction_value, /* y.transaction_type_name, */
      NULL AS mtl_cost_at_close,
      NULL AS unit_mtl_cost_at_close,
      (
        SELECT
          SUM(base_transaction_value)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.WIP_TRANSACTION_ACCOUNTS
          WHERE
            is_deleted_flg <> 'Y'
        ) AS a, (
          SELECT
            *
          FROM silver_bec_ods.wip_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS t
        WHERE
          a.transaction_id = t.transaction_id
          AND t.wip_entity_id = e.wip_entity_id
          AND t.resource_id = r.resource_id
          AND A.COST_ELEMENT_ID = 2
          AND a.accounting_line_type = 7
      ) AS mtl_oh_transaction_value,
      NULL AS mtl_oh_cost_at_close,
      NULL AS unit_mtl_oh_cost_at_close,
      (
        SELECT
          SUM(base_transaction_value)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.WIP_TRANSACTION_ACCOUNTS
          WHERE
            is_deleted_flg <> 'Y'
        ) AS a, (
          SELECT
            *
          FROM silver_bec_ods.wip_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS t
        WHERE
          a.transaction_id = t.transaction_id
          AND t.wip_entity_id = e.wip_entity_id
          AND t.resource_id = r.resource_id
          AND A.COST_ELEMENT_ID = 3
          AND a.accounting_line_type = 7
      ) AS res_transaction_value,
      NULL AS res_cost_at_close,
      NULL AS unit_res_cost_at_close,
      (
        SELECT
          SUM(base_transaction_value)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.WIP_TRANSACTION_ACCOUNTS
          WHERE
            is_deleted_flg <> 'Y'
        ) AS a, (
          SELECT
            *
          FROM silver_bec_ods.wip_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS t
        WHERE
          a.transaction_id = t.transaction_id
          AND t.wip_entity_id = e.wip_entity_id
          AND t.resource_id = r.resource_id
          AND A.COST_ELEMENT_ID = 4
          AND a.accounting_line_type = 7
      ) AS osp_transaction_value,
      NULL AS osp_cost_at_close,
      NULL AS unit_osp_cost_at_close,
      (
        SELECT
          SUM(base_transaction_value)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.WIP_TRANSACTION_ACCOUNTS
          WHERE
            is_deleted_flg <> 'Y'
        ) AS a, (
          SELECT
            *
          FROM silver_bec_ods.wip_transactions
          WHERE
            is_deleted_flg <> 'Y'
        ) AS t
        WHERE
          a.transaction_id = t.transaction_id
          AND t.wip_entity_id = e.wip_entity_id
          AND t.resource_id = r.resource_id
          AND A.COST_ELEMENT_ID = 5
          AND a.accounting_line_type = 7
      ) AS oh_transaction_value,
      NULL AS oh_cost_at_close,
      NULL AS unit_oh_cost_at_close,
      NULL AS cost_update_txn_value,
      NULL AS Mtl_variance,
      NULL AS mtl_oh_variance,
      NULL AS Res_variance,
      NULL AS osp_variance,
      NULL AS oh_variance,
      NULL AS net_value,
      d.organization_id AS organization_id1,
      NULL AS ALTERNATE_BOM,
      'Work Order Resources' AS COMMENTS
    FROM (
      SELECT
        *
      FROM silver_bec_ods.wip_entities
      WHERE
        is_deleted_flg <> 'Y'
    ) AS e, (
      SELECT
        *
      FROM silver_bec_ods.wip_discrete_jobs
      WHERE
        is_deleted_flg <> 'Y'
    ) AS d, (
      SELECT
        *
      FROM silver_bec_ods.WIP_OPERATION_RESOURCES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS r
    WHERE
      d.wip_entity_id = e.wip_entity_id
      AND d.wip_entity_id = r.wip_entity_id
      AND r.organization_id = d.organization_id
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wo_value_variance_union3_stg' AND batch_name = 'wip';