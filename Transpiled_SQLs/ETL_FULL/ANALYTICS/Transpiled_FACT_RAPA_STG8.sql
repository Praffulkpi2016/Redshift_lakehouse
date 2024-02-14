DROP table IF EXISTS gold_bec_dwh.FACT_RAPA_STG8;
CREATE TABLE gold_bec_dwh.FACT_RAPA_STG8 AS
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
    pr_open_qty,
    Primary_quantity,
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
    ) || '-' || COALESCE(inventory_item_id, 0) || '-' || COALESCE(requisition_number, 'NA') || '-' || COALESCE(cost_type, 'NA') || '-' || COALESCE(need_by_date, '1900-01-01 12:00:00') || '-' || COALESCE(pr_open_qty, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      item_id AS inventory_item_id,
      destination_organization_id AS organization_id,
      cost_type,
      NULL AS plan_name,
      'Receipt Forecast' AS data_type,
      'BLR Internal' AS order_group,
      'BLR Internal Forecast' AS order_type_text,
      pr.requisition_number,
      FLOOR(pr.need_by_date) AS need_by_date,
      0 AS aging_period,
      COALESCE(
        FLOOR(pr.schedule_ship_date),
        FLOOR(pr.need_by_date - CAST(COALESCE(pr.intransit_time, 0) AS INT))
      ) AS dock_promised_date,
      pr.part_number,
      pr.description,
      pr.planning_make_buy_code,
      pr_open_qty,
      CAST(NULL AS DECIMAL(38, 10)) AS primary_quantity,
      pr.unit_price,
      pr.primary_unit_of_measure,
      (
        pr_open_qty * pr.unit_price
      ) AS extended_cost,
      NULL AS po_line_type,
      'BLR INTERNAL' AS vendor_name,
      item_cost AS std_cost,
      item_cost * pr_open_qty AS ext_std_cost,
      (
        pr_open_qty * pr.unit_price - (
          item_cost
        ) * pr_open_qty
      ) AS variance,
      CAST(NULL AS DECIMAL(15, 0)) AS source_organization_id,
      planner_code,
      buyer_name,
      pr.unit_meas_lookup_code AS transactional_uom_code,
      CAST(NULL AS DECIMAL(15, 0)) AS release_num
    FROM (
      SELECT
        CASE WHEN cost_type_id = 1 THEN 'Frozen' WHEN cost_type_id = 3 THEN 'Pending' END AS cost_type,
        oola.ordered_quantity AS ord_quantity,
        oola.shipped_quantity AS so_ship_qty,
        oola.cancelled_quantity AS so_cancelled_qty,
        prl.destination_organization_id,
        prl.item_id,
        prl.deliver_to_location_id,
        prh.creation_date,
        prl.need_by_date,
        prh.segment1 || '-' || (
          ooha.order_number
        ) AS requisition_number,
        prl.requisition_header_id,
        prl.requisition_line_id,
        prl.quantity AS pr_quantity,
        prl.unit_price,
        msi.primary_unit_of_measure,
        (
          oola.ordered_quantity - COALESCE(oola.shipped_quantity, 0) - COALESCE(oola.cancelled_quantity, 0)
        ) AS pr_open_qty,
        NULL,
        msi.segment1 AS part_number,
        msi.description,
        msi.planner_code,
        CASE WHEN msi.planning_make_buy_code = 1 THEN 'Make' ELSE 'Buy' END AS planning_make_buy_code,
        oola.ordered_quantity,
        oola.cancelled_flag,
        oola.cancelled_quantity,
        oola.promise_date,
        oola.schedule_ship_date,
        oola.flow_status_code,
        cic.item_cost,
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
        (
          SELECT
            MAX(intransit_time)
          FROM (
            SELECT
              *
            FROM silver_bec_ods.mtl_interorg_ship_methods
            WHERE
              is_deleted_flg <> 'Y'
          )
          WHERE
            from_location_id = 144 AND to_location_id = prl.deliver_to_location_id
        ) AS intransit_time
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
        FROM silver_bec_ods.oe_order_headers_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ooha, (
        SELECT
          *
        FROM silver_bec_ods.cst_item_costs
        WHERE
          is_deleted_flg <> 'Y'
      ) AS cic
      WHERE
        prh.requisition_header_id = prl.requisition_header_id
        AND prh.type_lookup_code = 'INTERNAL'
        AND prl.item_id = msi.inventory_item_id
        AND prl.destination_organization_id = msi.organization_id
        AND prl.requisition_line_id = oola.SOURCE_DOCUMENT_LINE_ID()
        AND COALESCE(prl.cancel_flag, 'N') = 'N'
        AND COALESCE(oola.cancelled_flag, 'N') = 'N'
        AND oola.cancelled_quantity = 0
        AND oola.flow_status_code <> 'CLOSED'
        AND (
          oola.ordered_quantity - COALESCE(oola.shipped_quantity, 0) - COALESCE(oola.cancelled_quantity, 0)
        ) > 0
        AND oola.header_id = ooha.HEADER_ID()
        AND prl.destination_organization_id = cic.ORGANIZATION_ID()
        AND prl.item_id = cic.INVENTORY_ITEM_ID()
        AND cost_type_id IN (1, 3)
    ) AS pr
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_rapa_stg8' AND batch_name = 'ascp';