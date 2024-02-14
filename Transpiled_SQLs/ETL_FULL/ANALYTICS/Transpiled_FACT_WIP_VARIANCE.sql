DROP table IF EXISTS gold_bec_dwh.FACT_WIP_VARIANCE;
CREATE TABLE gold_bec_dwh.FACT_WIP_VARIANCE AS
(
  SELECT
    transaction_id,
    organization_id,
    wip_entity_id,
    wip_entity_name,
    primary_item_id,
    line_id,
    line_code,
    acct_period_id,
    trx_type_name,
    transaction_date,
    TRANSACTION_QUANTITY,
    transaction_uom,
    PRIMARY_QUANTITY,
    primary_uom,
    operation_seq_num,
    CURRENCY_CODE,
    CURRENCY_CONVERSION_DATE,
    CURRENCY_CONVERSION_TYPE,
    CURRENCY_CONVERSION_RATE,
    department_id,
    reason_name,
    reference,
    INVENTORY_ITEM_ID,
    REVISION,
    SUBINVENTORY_CODE,
    resource_seq_num,
    reference_account,
    resource_id,
    repetitive_schedule_id,
    LINE_TYPE_NAME,
    transaction_value,
    base_transaction_value,
    contra_set_id,
    basis,
    COST_ELEMENT,
    ACTIVITY,
    rate_or_amount,
    gl_batch_id,
    overhead_basis_factor,
    basis_resource_id,
    TRANSACTION_SOURCE,
    UNIT_COST,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    asset_number,
    asset_group_id,
    rebuild_item_id,
    rebuild_serial_number,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || transaction_id AS transaction_id_KEY,
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
    ) || '-' || wip_entity_id AS wip_entity_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || department_id AS department_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || line_id AS line_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || resource_id AS resource_id_KEY,
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
    ) || '-' || COALESCE(transaction_id, 0) || '-' || COALESCE(wip_entity_id, 0) || '-' || COALESCE(reference_account, 0) || '-' || COALESCE(COST_ELEMENT, 'NA') || '-' || COALESCE(resource_id, 0) || '-' || COALESCE(base_transaction_value, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    WITH wip_entitity AS (
      SELECT
        we.organization_id,
        we.wip_entity_name,
        we.primary_item_id,
        we.entity_type,
        wdj.wip_entity_id,
        wdj.rebuild_serial_number,
        wdj.rebuild_item_id AS rebuild_item_id,
        wdj.asset_group_id AS asset_group_id,
        wdj.asset_number AS asset_number
      FROM (
        SELECT
          *
        FROM silver_bec_ods.wip_entities
        WHERE
          is_deleted_flg <> 'Y'
      ) AS we, (
        SELECT
          *
        FROM silver_bec_ods.wip_discrete_jobs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS wdj
      WHERE
        we.wip_entity_id = wdj.wip_entity_id AND we.organization_id = wdj.organization_id
    ), cst_activity AS (
      SELECT
        activity_id,
        activity
      FROM silver_bec_ods.cst_activities
      WHERE
        is_deleted_flg <> 'Y'
    ), trx_reasons AS (
      SELECT
        reason_id,
        reason_name
      FROM silver_bec_ods.mtl_transaction_reasons
      WHERE
        is_deleted_flg <> 'Y'
    ), csi AS (
      SELECT
        instance_number,
        serial_number,
        inventory_item_id
      FROM silver_bec_ods.csi_item_instances
      WHERE
        is_deleted_flg <> 'Y'
    )
    (
      SELECT
        wt.transaction_id,
        we.organization_id,
        we.wip_entity_id,
        we.wip_entity_name AS wip_entity_name,
        we.primary_item_id,
        wt.line_id,
        NULL AS line_code,
        wt.acct_period_id,
        lu1.meaning AS trx_type_name,
        wt.transaction_date,
        CASE
          WHEN wt.transaction_type = 11
          THEN wta.primary_quantity
          WHEN wt.transaction_type = 12
          THEN wta.primary_quantity
          WHEN wt.transaction_type = 14
          THEN wta.primary_quantity
          ELSE wt.transaction_quantity
        END AS TRANSACTION_QUANTITY,
        wt.transaction_uom,
        CASE
          WHEN wt.transaction_type = 11
          THEN wta.primary_quantity
          WHEN wt.transaction_type = 12
          THEN wta.primary_quantity
          WHEN wt.transaction_type = 14
          THEN wta.primary_quantity
          ELSE CASE
            WHEN wta.base_transaction_value = 0
            THEN wta.primary_quantity
            ELSE SIGN(wta.base_transaction_value) * ABS(wta.primary_quantity)
          END
        END AS PRIMARY_QUANTITY,
        wt.primary_uom,
        wt.operation_seq_num,
        COALESCE(wta.currency_code, wt.currency_code) AS CURRENCY_CODE,
        COALESCE(wta.currency_conversion_date, wt.currency_conversion_date) AS CURRENCY_CONVERSION_DATE,
        COALESCE(wta.currency_conversion_type, wt.currency_conversion_type) AS CURRENCY_CONVERSION_TYPE,
        COALESCE(wta.currency_conversion_rate, wt.currency_conversion_rate) AS CURRENCY_CONVERSION_RATE,
        CASE
          WHEN we.entity_type = 6
          THEN COALESCE(wt.charge_department_id, wt.department_id)
          WHEN we.entity_type = 7
          THEN COALESCE(wt.charge_department_id, wt.department_id)
          ELSE wt.department_id
        END AS department_id,
        mtr.reason_name,
        wt.reference,
        -1 AS INVENTORY_ITEM_ID,
        NULL AS REVISION,
        NULL AS SUBINVENTORY_CODE,
        wt.resource_seq_num,
        wta.reference_account,
        wta.resource_id,
        wta.repetitive_schedule_id,
        lu2.meaning AS LINE_TYPE_NAME,
        wta.transaction_value,
        wta.base_transaction_value,
        wta.contra_set_id,
        lu3.meaning AS basis,
        CASE
          WHEN wta.cost_element_id = 1
          THEN 'Material'
          WHEN wta.cost_element_id = 2
          THEN 'Material Overhead'
          WHEN wta.cost_element_id = 3
          THEN 'Resource'
          WHEN wta.cost_element_id = 4
          THEN 'Outside Processing'
          WHEN wta.cost_element_id = 5
          THEN 'Overhead'
          ELSE ' '
        END AS COST_ELEMENT,
        ca.activity AS ACTIVITY,
        wta.rate_or_amount,
        wta.gl_batch_id,
        wta.overhead_basis_factor,
        wta.basis_resource_id,
        poh.segment1 AS TRANSACTION_SOURCE,
        CASE
          WHEN wta.primary_quantity IS NULL
          THEN NULL
          WHEN wta.primary_quantity = 0
          THEN NULL
          ELSE CASE
            WHEN wta.rate_or_amount IS NULL
            THEN ABS(wta.base_transaction_value / wta.primary_quantity)
            ELSE ABS(wta.rate_or_amount)
          END
        END AS UNIT_COST,
        wt.last_update_date,
        wt.last_updated_by,
        wt.creation_date,
        wt.created_by,
        wt.last_update_login,
        wt.request_id,
        wt.program_application_id,
        wt.program_id,
        wt.program_update_date,
        cii1.instance_number AS asset_number,
        we.asset_group_id,
        we.rebuild_item_id,
        cii2.instance_number AS rebuild_serial_number
      FROM (
        SELECT
          *
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          is_deleted_flg <> 'Y'
      ) AS lu3, (
        SELECT
          *
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          is_deleted_flg <> 'Y'
      ) AS lu2, (
        SELECT
          *
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          is_deleted_flg <> 'Y'
      ) AS lu1, (
        SELECT
          *
        FROM silver_bec_ods.po_headers_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS poh, (
        SELECT
          *
        FROM silver_bec_ods.wip_transaction_accounts
        WHERE
          is_deleted_flg <> 'Y'
      ) AS wta, (
        SELECT
          *
        FROM silver_bec_ods.wip_transactions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS wt, wip_entitity AS we, trx_reasons AS mtr, cst_activity AS ca, csi AS cii1, csi AS cii2
      WHERE
        wta.transaction_id = wt.transaction_id
        AND we.wip_entity_id = wta.wip_entity_id
        AND we.organization_id = wt.organization_id
        AND lu1.LOOKUP_TYPE() = 'WIP_TRANSACTION_TYPE'
        AND lu1.LOOKUP_CODE() = wt.transaction_type
        AND mtr.REASON_ID() = wt.reason_id
        AND poh.PO_HEADER_ID() = wt.po_header_id
        AND lu2.lookup_type = 'CST_ACCOUNTING_LINE_TYPE'
        AND lu2.lookup_code = wta.accounting_line_type
        AND lu3.LOOKUP_CODE() = wta.basis_type
        AND lu3.LOOKUP_TYPE() = 'CST_BASIS'
        AND ca.ACTIVITY_ID() = wta.activity_id
        AND cii1.SERIAL_NUMBER() = we.asset_number
        AND cii1.INVENTORY_ITEM_ID() = we.asset_group_id
        AND cii2.SERIAL_NUMBER() = we.rebuild_serial_number
        AND cii2.INVENTORY_ITEM_ID() = we.rebuild_item_id
        AND lu2.meaning = 'WIP valuation' /* and we.organization_id = 106 and wip_entity_name = '2301070436-POR-144522' */
      UNION
      SELECT
        mmt.transaction_id,
        we.organization_id,
        we.wip_entity_id,
        we.wip_entity_name,
        we.primary_item_id,
        mmt.repetitive_line_id AS LINE_ID,
        NULL AS line_code,
        mmt.acct_period_id,
        mtt.transaction_type_name AS TRANSACTION_TYPE_NAME,
        mmt.transaction_date,
        CASE
          WHEN mmt.transaction_action_id = 40
          THEN mta.primary_quantity
          WHEN mmt.transaction_action_id = 41
          THEN mta.primary_quantity
          WHEN mmt.transaction_action_id = 43
          THEN mta.primary_quantity
          ELSE -mmt.transaction_quantity
        END AS TRANSACTION_QUANTITY,
        mmt.transaction_uom,
        mta.primary_quantity,
        msi.primary_uom_code,
        mmt.operation_seq_num,
        COALESCE(mta.currency_code, mmt.currency_code) AS CURRENCY_CODE,
        COALESCE(mta.currency_conversion_date, mmt.currency_conversion_date) AS CURRENCY_CONVERSION_DATE,
        COALESCE(mta.currency_conversion_type, mmt.currency_conversion_type) AS CURRENCY_CONVERSION_TYPE,
        COALESCE(mta.currency_conversion_rate, mmt.currency_conversion_rate) AS CURRENCY_CONVERSION_RATE,
        mmt.department_id,
        mtr.reason_name,
        mmt.transaction_reference AS REFERENCE,
        mmt.inventory_item_id AS INVENTORY_ITEM_ID,
        mmt.revision,
        mmt.subinventory_code,
        NULL AS RESOURCE_SEQ_NUM,
        mta.reference_account AS reference_account,
        mta.resource_id,
        mta.repetitive_schedule_id,
        lu2.meaning AS LINE_TYPE_NAME,
        mta.transaction_value,
        mta.base_transaction_value,
        mta.contra_set_id,
        '' AS BASIS,
        CASE
          WHEN mta.cost_element_id = 1
          THEN 'Material'
          WHEN mta.cost_element_id = 2
          THEN 'Material Overhead'
          WHEN mta.cost_element_id = 3
          THEN 'Resource'
          WHEN mta.cost_element_id = 4
          THEN 'Outside Processing'
          WHEN mta.cost_element_id = 5
          THEN 'Overhead'
        END AS COST_ELEMENT,
        ca.activity,
        mta.rate_or_amount,
        mta.gl_batch_id,
        -1 AS OVERHEAD_BASIS_FACTOR,
        -1 AS BASIS_RESOURCE_ID,
        NULL AS TRANSACTION_SOURCE,
        CASE
          WHEN mmt.primary_quantity IS NULL
          THEN NULL
          WHEN mmt.primary_quantity = 0
          THEN NULL
          ELSE CASE
            WHEN mta.rate_or_amount IS NULL
            THEN ABS(
              mta.base_transaction_value / CASE
                WHEN mmt.transaction_action_id = 40
                THEN mta.primary_quantity
                WHEN mmt.transaction_action_id = 41
                THEN mta.primary_quantity
                WHEN mmt.transaction_action_id = 43
                THEN mta.primary_quantity
                ELSE mmt.primary_quantity
              END
            )
            ELSE ABS(mta.rate_or_amount)
          END
        END AS UNIT_COST,
        mmt.last_update_date,
        mmt.last_updated_by,
        mmt.creation_date,
        mmt.created_by,
        mmt.last_update_login,
        mmt.request_id,
        mmt.program_application_id,
        mmt.program_id,
        mmt.program_update_date,
        cii1.instance_number AS asset_number,
        we.asset_group_id,
        we.rebuild_item_id,
        cii2.instance_number AS rebuild_serial_number
      FROM (
        SELECT
          *
        FROM silver_bec_ods.mtl_transaction_types
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mtt, (
        SELECT
          *
        FROM silver_bec_ods.mtl_system_items_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS msi, (
        SELECT
          *
        FROM silver_bec_ods.fnd_lookup_values
        WHERE
          is_deleted_flg <> 'Y'
      ) AS lu2, (
        SELECT
          *
        FROM silver_bec_ods.mtl_transaction_accounts
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mta, (
        SELECT
          *
        FROM silver_bec_ods.mtl_material_transactions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mmt, wip_entitity AS we, trx_reasons AS mtr, cst_activity AS ca, csi AS cii1, csi AS cii2
      WHERE
        mta.transaction_source_id = we.wip_entity_id
        AND mmt.organization_id = we.organization_id
        AND mtt.transaction_type_id = mmt.transaction_type_id
        AND mtr.REASON_ID() = mmt.reason_id
        AND mta.transaction_source_type_id = 5
        AND mmt.transaction_source_type_id = 5
        AND mta.transaction_id = mmt.transaction_id
        AND lu2.lookup_type = 'CST_ACCOUNTING_LINE_TYPE'
        AND lu2.lookup_code = mta.accounting_line_type
        AND msi.inventory_item_id = mmt.inventory_item_id
        AND msi.organization_id = mta.organization_id
        AND ca.ACTIVITY_ID() = mta.activity_id
        AND cii1.SERIAL_NUMBER() = we.asset_number
        AND cii1.INVENTORY_ITEM_ID() = we.asset_group_id
        AND cii2.SERIAL_NUMBER() = we.rebuild_serial_number
        AND cii2.INVENTORY_ITEM_ID() = we.rebuild_item_id
        AND lu2.meaning = 'WIP valuation'
    )
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wip_variance' AND batch_name = 'wip';