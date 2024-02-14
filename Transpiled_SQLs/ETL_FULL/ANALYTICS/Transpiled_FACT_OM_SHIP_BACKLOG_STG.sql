DROP table IF EXISTS gold_bec_dwh.FACT_OM_SHIP_BACKLOG_STG;
CREATE TABLE gold_bec_dwh.FACT_OM_SHIP_BACKLOG_STG AS
(
  SELECT
    bill_to_customer,
    ship_to_customer,
    order_number,
    line_number,
    item,
    item_description,
    ordered_quantity,
    shipped_quantity,
    backlog_quantity,
    actual_shipped_date,
    schedule_ship_date,
    pick_list,
    delivery,
    waybill,
    serial_number,
    order_amnt,
    shipment_amnt,
    to_go_shipments,
    support_reference,
    organization_id,
    component_number,
    site_address,
    cust_po_number,
    shipment_number,
    header_id,
    line_id,
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
    ) || '-' || header_id AS header_id_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || line_id AS line_id_KEY,
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
    ) || '-' || COALESCE(item, 'NA') || '-' || COALESCE(shipment_number, 0) || '-' || COALESCE(organization_id, 0) || '-' || COALESCE(order_number, 'NA') || '-' || COALESCE(line_number, 0) || '-' || COALESCE(serial_number, 'NA') || '-' || COALESCE(delivery, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    WITH oe_lines AS (
      SELECT
        oeh.header_id,
        oeh.org_id,
        oeh.cust_po_number,
        oeh.order_number AS order_number,
        ola.line_id,
        ola.shipment_number,
        ola.inventory_item_id,
        ola.component_number,
        ola.ordered_quantity,
        ola.item_type_code,
        ola.ship_to_org_id,
        ola.invoice_to_org_id,
        ola.line_number,
        ola.fulfilled_quantity,
        ola.fulfillment_date,
        ola.actual_shipment_date,
        schedule_ship_date,
        ola.unit_selling_price
      FROM (
        SELECT
          *
        FROM silver_bec_ods.oe_order_headers_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS oeh, (
        SELECT
          *
        FROM silver_bec_ods.oe_order_lines_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS ola
      WHERE
        oeh.header_id = ola.header_id
        AND ola.item_type_code IN ('INCLUDED', 'OPTION', 'MODEL')
        AND oeh.org_id = CASE WHEN ola.item_type_code = 'MODEL' THEN 85 ELSE oeh.org_id END
    ), bec_cst_details AS (
      SELECT
        site_use_id,
        party_name,
        address1,
        address2,
        address3,
        address4,
        address5,
        address1 || ' ' || address2 || ' ' || address3 || ' ' || address4 || ' ' || address5 AS site_address
      FROM silver_bec_ods.bec_customer_details_view
      WHERE
        is_deleted_flg <> 'Y'
    ), wsh_pick AS (
      SELECT
        pick_slip_number,
        move_order_line_id
      FROM silver_bec_ods.mtl_material_transactions_temp
      WHERE
        is_deleted_flg <> 'Y'
        AND NOT pick_slip_number IS NULL
        AND ABS(COALESCE(transaction_quantity, 0)) > 0
      UNION ALL
      SELECT
        pick_slip_number,
        move_order_line_id
      FROM silver_bec_ods.mtl_material_transactions
      WHERE
        is_deleted_flg <> 'Y'
        AND NOT pick_slip_number IS NULL
        AND COALESCE(transaction_quantity, 0) < 0
      UNION ALL
      SELECT
        pick_slip_number,
        line_id AS move_order_line_id
      FROM silver_bec_ods.mtl_txn_request_lines
      WHERE
        is_deleted_flg <> 'Y' AND NOT pick_slip_number IS NULL
    ), wsh_pick_slip AS (
      SELECT
        pick_slip_number,
        move_order_line_id,
        ROW_NUMBER() OVER (ORDER BY move_order_line_id NULLS LAST) AS rn
      FROM wsh_pick
    )
    SELECT
      bcd.party_name AS bill_to_customer,
      ship.party_name AS ship_to_customer,
      CAST(ola.order_number AS STRING) AS order_number,
      ola.line_number,
      msi.segment1 AS item,
      msi.description AS item_description,
      ola.ordered_quantity,
      0 AS shipped_quantity,
      ola.ordered_quantity AS backlog_quantity,
      ola.actual_shipment_date AS actual_shipped_date,
      ola.schedule_ship_date AS schedule_ship_date,
      NULL AS pick_list,
      NULL AS delivery,
      NULL AS waybill,
      NULL AS serial_number,
      NULL AS order_amnt,
      NULL AS shipment_amnt,
      NULL AS to_go_shipments,
      NULL AS support_reference,
      msi.organization_id AS organization_id,
      ola.component_number,
      ship.site_address,
      ola.cust_po_number,
      ola.shipment_number,
      ola.header_id,
      ola.line_id
    FROM oe_lines AS ola, bec_cst_details AS bcd, bec_cst_details AS ship, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS msi
    WHERE
      bcd.SITE_USE_ID() = ola.invoice_to_org_id
      AND ship.SITE_USE_ID() = ola.ship_to_org_id
      AND ola.inventory_item_id = msi.inventory_item_id
      AND ola.item_type_code IN ('INCLUDED', 'OPTION')
      AND ola.ordered_quantity > 0
      AND ola.actual_shipment_date IS NULL
    UNION
    SELECT
      bcd.party_name AS bill_to_customer,
      ship.party_name AS ship_to_customer,
      CAST(ola.order_number AS STRING),
      ola.line_number,
      msi.segment1 AS item,
      msi.description AS item_description,
      ola.ordered_quantity,
      COALESCE(ola.fulfilled_quantity, 0) AS shipped_quantity,
      COALESCE((
        ola.ordered_quantity - COALESCE(ola.fulfilled_quantity, 0)
      ), 0) AS backlog_quantity,
      ola.fulfillment_date AS actual_shipped_date,
      schedule_ship_date,
      NULL AS pick_list,
      NULL AS delivery,
      NULL AS waybill,
      NULL AS serial_number,
      ola.unit_selling_price AS order_amnt,
      COALESCE((
        ola.unit_selling_price * ola.fulfilled_quantity
      ), 0) AS shipment_amnt,
      COALESCE(
        (
          ola.unit_selling_price - COALESCE((
            ola.unit_selling_price * ola.fulfilled_quantity
          ), 0)
        ),
        0
      ) AS to_go_shipments,
      NULL AS support_reference,
      msi.organization_id AS organization_id,
      0 AS component_number,
      ship.site_address,
      ola.cust_po_number,
      ola.shipment_number,
      ola.header_id,
      ola.line_id
    FROM oe_lines AS ola, bec_cst_details AS bcd, bec_cst_details AS ship, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS msi
    WHERE
      ship.SITE_USE_ID() = ola.ship_to_org_id
      AND bcd.SITE_USE_ID() = ola.invoice_to_org_id
      AND ola.inventory_item_id = msi.inventory_item_id
      AND ola.ordered_quantity > 0
      AND ola.item_type_code = 'MODEL'
    UNION
    SELECT
      bcd.party_name AS bill_to_customer,
      ship.party_name AS ship_to_customer,
      CAST(wdd.source_header_number AS STRING) AS order_number,
      ola.line_number,
      msi.segment1 AS item,
      wdd.item_description AS item_description,
      1 AS ordered_quantity,
      1 AS shipped_quantity,
      0 AS backlog_quantity,
      ola.actual_shipment_date AS actual_shipped_date,
      wdd.date_scheduled AS schedule_ship_date,
      (
        SELECT
          pick_slip_number
        FROM wsh_pick_slip AS pick
        WHERE
          pick.move_order_line_id = wdd.move_order_line_id AND rn < 2
      ) AS pick_list,
      wnd.name AS delivery,
      waybill,
      wsn.fm_serial_number AS serial_number,
      NULL AS order_amnt,
      NULL AS shipment_amnt,
      NULL AS to_go_shipments,
      NULL AS support_reference,
      wnd.organization_id,
      ola.component_number,
      ship.site_address,
      ola.cust_po_number,
      ola.shipment_number,
      ola.header_id,
      ola.line_id
    FROM oe_lines AS ola, bec_cst_details AS bcd, bec_cst_details AS ship, (
      SELECT
        *
      FROM silver_bec_ods.wsh_serial_numbers
      WHERE
        is_deleted_flg <> 'Y'
    ) AS wsn, (
      SELECT
        *
      FROM silver_bec_ods.wsh_new_deliveries
      WHERE
        is_deleted_flg <> 'Y'
    ) AS wnd, (
      SELECT
        *
      FROM silver_bec_ods.wsh_delivery_details
      WHERE
        is_deleted_flg <> 'Y'
    ) AS wdd, (
      SELECT
        *
      FROM silver_bec_ods.wsh_delivery_assignments
      WHERE
        is_deleted_flg <> 'Y'
    ) AS wda, (
      SELECT
        *
      FROM silver_bec_ods.mtl_system_items_b
      WHERE
        is_deleted_flg <> 'Y'
    ) AS msi
    WHERE
      wda.delivery_detail_id = wdd.delivery_detail_id
      AND COALESCE(wda.type, 'S') IN ('S', 'C')
      AND COALESCE(wdd.line_direction, 'O') IN ('O', 'IO')
      AND wnd.delivery_id = wda.DELIVERY_ID()
      AND wdd.source_line_id = ola.line_id
      AND wdd.source_header_id = ola.header_id
      AND wdd.delivery_detail_id = wsn.DELIVERY_DETAIL_ID()
      AND wdd.inventory_item_id = msi.inventory_item_id
      AND wdd.organization_id = msi.organization_id
      AND ship.SITE_USE_ID() = ola.ship_to_org_id
      AND ola.invoice_to_org_id = bcd.SITE_USE_ID()
      AND ola.item_type_code IN ('INCLUDED', 'OPTION')
      AND ola.ordered_quantity > 0
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_om_ship_backlog_stg' AND batch_name = 'om';