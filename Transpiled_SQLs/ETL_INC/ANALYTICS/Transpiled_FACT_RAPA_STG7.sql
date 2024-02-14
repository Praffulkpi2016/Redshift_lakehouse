TRUNCATE table gold_bec_dwh.FACT_RAPA_STG7;
INSERT INTO gold_bec_dwh.FACT_RAPA_STG7
(
  SELECT
    inventory_item_id,
    organization_id,
    cost_type,
    plan_name,
    data_type,
    order_group,
    order_type_text,
    requisition_number,
    need_by_date,
    aging_period,
    dock_promised_date,
    part_number,
    description,
    planning_make_buy_code,
    NULL AS category_name,
    so_ship_qty,
    primary_quantity,
    unit_price,
    primary_unit_of_measure,
    extended_cost,
    po_line_type,
    vendor_name,
    std_cost,
    ext_std_cost,
    variance,
    source_organization_id,
    planner_code,
    buyer_name,
    transactional_uom_code,
    release_num,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || inventory_item_id AS inventory_item_id_KEY,
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
    ) || '-' || source_organization_id AS source_organization_id_KEY,
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
    ) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(dock_promised_date, '1900-01-01 12:00:00') || '-' || COALESCE(cost_type, 'NA') || '-' || COALESCE(need_by_date, '1900-01-01 12:00:00') || '-' || COALESCE(so_ship_qty, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      pr.inventory_item_id,
      pr.destination_organization_id AS organization_id,
      pr.cost_type,
      NULL AS plan_name,
      'Receipt Actual' AS data_type,
      'BLR Internal' AS order_group,
      'BLR Internal Actuals' AS order_type_text,
      pr.requisition_number,
      FLOOR(pr.need_by_date) AS need_by_date,
      0 AS aging_period,
      FLOOR(transaction_date) AS dock_promised_date,
      pr.part_number,
      pr.description,
      pr.planning_make_buy_code,
      so_ship_qty,
      CAST(NULL AS DECIMAL(38, 10)) AS primary_quantity,
      pr.unit_price,
      pr.primary_unit_of_measure,
      so_ship_qty * pr.unit_price AS extended_cost,
      NULL AS po_line_type,
      'BLR INTERNAL' AS vendor_name,
      ch.standard_cost AS std_cost,
      ch.standard_cost * so_ship_qty AS ext_std_cost,
      (
        SELECT
          SUM(base_transaction_value)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.mtl_transaction_accounts
          WHERE
            is_deleted_flg <> 'Y'
        ) AS mta, (
          SELECT
            *
          FROM silver_bec_ods.gl_code_combinations_kfv
          WHERE
            is_deleted_flg <> 'Y'
        ) AS gcc
        WHERE
          mta.reference_account = gcc.code_combination_id
          AND transaction_id = pr.transaction_id
          AND gcc.segment3 = '52103'
      ) AS variance,
      CAST(NULL AS DECIMAL(15, 0)) AS source_organization_id,
      planner_code,
      buyer_name,
      pr.unit_meas_lookup_code AS transactional_uom_code,
      CAST(NULL AS DECIMAL(15, 0)) AS release_num
    FROM (
      SELECT
        'Frozen' AS cost_type,
        oola.line_id,
        mmt.inventory_item_id,
        mmt.transaction_date,
        (
          SELECT
            MAX(cost_update_id)
          FROM (
            SELECT
              *
            FROM silver_bec_ods.cst_cost_history_v
            WHERE
              is_deleted_flg <> 'Y'
          ) AS ch
          WHERE
            ch.inventory_item_id = mmt.inventory_item_id
            AND ch.last_update_date <= mmt.transaction_date
            AND ch.organization_id = prl.destination_organization_id
        ) AS cost_update_id,
        mmt.transaction_id,
        ABS(mmt.transaction_quantity) AS so_ship_qty,
        prl.destination_organization_id,
        prl.item_id,
        prl.deliver_to_location_id,
        prh.creation_date,
        prl.need_by_date,
        prh.segment1 || '-' || (
          SELECT
            ooha.order_number
          FROM (
            SELECT
              *
            FROM silver_bec_ods.oe_order_headers_all
            WHERE
              is_deleted_flg <> 'Y'
          ) AS ooha
          WHERE
            ooha.header_id = oola.header_id
        ) AS requisition_number,
        prl.requisition_header_id,
        prl.requisition_line_id,
        prl.quantity AS pr_quantity,
        prl.unit_price,
        msi.segment1 AS part_number,
        msi.description,
        msi.planner_code,
        CASE WHEN msi.planning_make_buy_code = 1 THEN 'Make' ELSE 'Buy' END AS planning_make_buy_code,
        oola.ordered_quantity,
        oola.cancelled_flag,
        oola.cancelled_quantity,
        oola.promise_date,
        oola.schedule_ship_date,
        (
          SELECT
            agent_name
          FROM (
            SELECT
              *
            FROM silver_bec_ods.po_agents_v
            WHERE
              is_deleted_flg <> 'Y'
          ) AS poa
          WHERE
            poa.agent_id = COALESCE(prl.suggested_buyer_id, msi.buyer_id)
        ) AS buyer_name,
        prl.unit_meas_lookup_code,
        msi.primary_unit_of_measure
      FROM (
        SELECT
          *
        FROM silver_bec_ods.po_requisition_headers_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS prh, (
        SELECT
          *
        FROM silver_bec_ods.po_requisition_lines_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS prl, (
        SELECT
          *
        FROM silver_bec_ods.mtl_system_items_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS msi, (
        SELECT
          *
        FROM silver_bec_ods.oe_order_lines_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS oola, (
        SELECT
          *
        FROM silver_bec_ods.mtl_material_transactions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mmt
      WHERE
        prh.requisition_header_id = prl.requisition_header_id
        AND prl.item_id = msi.inventory_item_id
        AND oola.line_id = mmt.source_line_id
        AND oola.inventory_item_id = mmt.inventory_item_id
        AND mmt.transaction_type_id = 62
        AND mmt.organization_id = 101
        AND mmt.transaction_date > CURRENT_TIMESTAMP() - 365
        AND prl.destination_organization_id = msi.organization_id
        AND prl.requisition_line_id = oola.SOURCE_DOCUMENT_LINE_ID()
        AND prh.type_lookup_code = 'INTERNAL'
        AND COALESCE(prl.cancel_flag, 'N') = 'N'
        AND COALESCE(oola.cancelled_flag, 'N') = 'N'
        AND COALESCE(oola.shipped_quantity, 0) > 0
      UNION ALL
      SELECT
        'Pending' AS cost_type,
        oola.line_id,
        mmt.inventory_item_id,
        mmt.transaction_date,
        (
          SELECT
            MAX(cost_update_id)
          FROM (
            SELECT
              *
            FROM silver_bec_ods.cst_cost_history_v
            WHERE
              is_deleted_flg <> 'Y'
          ) AS ch
          WHERE
            ch.inventory_item_id = mmt.inventory_item_id
            AND ch.last_update_date <= mmt.transaction_date
            AND ch.organization_id = prl.destination_organization_id
        ) AS cost_update_id,
        mmt.transaction_id,
        ABS(mmt.transaction_quantity) AS so_ship_qty,
        prl.destination_organization_id,
        prl.item_id,
        prl.deliver_to_location_id,
        prh.creation_date,
        prl.need_by_date,
        prh.segment1 || '-' || (
          SELECT
            ooha.order_number
          FROM (
            SELECT
              *
            FROM silver_bec_ods.oe_order_headers_all
            WHERE
              is_deleted_flg <> 'Y'
          ) AS ooha
          WHERE
            ooha.header_id = oola.header_id
        ) AS requisition_number,
        prl.requisition_header_id,
        prl.requisition_line_id,
        prl.quantity AS pr_quantity,
        prl.unit_price,
        msi.segment1 AS part_number,
        msi.description,
        msi.planner_code,
        CASE WHEN msi.planning_make_buy_code = 1 THEN 'Make' ELSE 'Buy' END AS planning_make_buy_code,
        oola.ordered_quantity,
        oola.cancelled_flag,
        oola.cancelled_quantity,
        oola.promise_date,
        oola.schedule_ship_date,
        (
          SELECT
            agent_name
          FROM (
            SELECT
              *
            FROM silver_bec_ods.po_agents_v
            WHERE
              is_deleted_flg <> 'Y'
          ) AS poa
          WHERE
            poa.agent_id = COALESCE(prl.suggested_buyer_id, msi.buyer_id)
        ) AS buyer_name,
        prl.unit_meas_lookup_code,
        msi.primary_unit_of_measure
      FROM (
        SELECT
          *
        FROM silver_bec_ods.po_requisition_headers_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS prh, (
        SELECT
          *
        FROM silver_bec_ods.po_requisition_lines_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS prl, (
        SELECT
          *
        FROM silver_bec_ods.mtl_system_items_b
        WHERE
          is_deleted_flg <> 'Y'
      ) AS msi, (
        SELECT
          *
        FROM silver_bec_ods.oe_order_lines_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS oola, (
        SELECT
          *
        FROM silver_bec_ods.mtl_material_transactions
        WHERE
          is_deleted_flg <> 'Y'
      ) AS mmt
      WHERE
        prh.requisition_header_id = prl.requisition_header_id
        AND prl.item_id = msi.inventory_item_id
        AND oola.line_id = mmt.source_line_id
        AND oola.inventory_item_id = mmt.inventory_item_id
        AND mmt.transaction_type_id = 62
        AND mmt.organization_id = 101
        AND mmt.transaction_date > CURRENT_TIMESTAMP() - 365
        AND prl.destination_organization_id = msi.organization_id
        AND prl.requisition_line_id = oola.SOURCE_DOCUMENT_LINE_ID()
        AND prh.type_lookup_code = 'INTERNAL'
        AND COALESCE(prl.cancel_flag, 'N') = 'N'
        AND COALESCE(oola.cancelled_flag, 'N') = 'N'
        AND COALESCE(oola.shipped_quantity, 0) > 0
    ) AS pr, (
      SELECT
        *
      FROM silver_bec_ods.cst_cost_history_v
      WHERE
        is_deleted_flg <> 'Y'
    ) AS ch
    WHERE
      pr.cost_update_id = ch.COST_UPDATE_ID()
      AND pr.inventory_item_id = ch.INVENTORY_ITEM_ID()
      AND pr.destination_organization_id = ch.ORGANIZATION_ID()
      AND pr.transaction_date >= ch.LAST_UPDATE_DATE()
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_rapa_stg7' AND batch_name = 'ascp';