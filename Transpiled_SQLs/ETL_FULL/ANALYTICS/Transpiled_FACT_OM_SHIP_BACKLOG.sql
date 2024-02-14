DROP table IF EXISTS gold_bec_dwh.FACT_OM_SHIP_BACKLOG;
CREATE TABLE gold_bec_dwh.FACT_OM_SHIP_BACKLOG AS
(
  WITH ship AS (
    SELECT DISTINCT
      o.order_number AS order_ship,
      FLOOR(COALESCE(l.actual_shipment_date, l.fulfillment_date)) AS ship_filter
    FROM (
      SELECT
        *
      FROM silver_bec_ods.oe_order_lines_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS l, (
      SELECT
        *
      FROM silver_bec_ods.oe_order_headers_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS o
    WHERE
      o.header_id = l.header_id AND l.item_type_code IN ('MODEL', 'INCLUDED', 'OPTION')
  ), rma AS (
    SELECT
      order_number AS rma_number,
      reference_header_id,
      reference_line_id,
      return_attribute2
    FROM (
      SELECT
        *
      FROM silver_bec_ods.oe_order_lines_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS rma_oel, (
      SELECT
        *
      FROM silver_bec_ods.oe_order_headers_all
      WHERE
        is_deleted_flg <> 'Y'
    ) AS rma_oeh
    WHERE
      rma_oel.header_id = rma_oeh.header_id
      AND rma_oeh.order_type_id IN (
        SELECT
          transaction_type_id
        FROM (
          SELECT
            *
          FROM silver_bec_ods.oe_transaction_types_tl
          WHERE
            is_deleted_flg <> 'Y'
        )
        WHERE
          name = 'Customer Service Swap'
      )
  )
  SELECT
    ship.ship_filter,
    'Q1' AS filter1,
    a.BILL_TO_CUSTOMER,
    a.SHIP_TO_CUSTOMER,
    a.ORDER_NUMBER,
    a.LINE_NUMBER,
    a.ITEM,
    a.ITEM_DESCRIPTION,
    a.ORDERED_QUANTITY,
    a.SHIPPED_QUANTITY,
    a.BACKLOG_QUANTITY,
    a.ACTUAL_SHIPPED_DATE,
    a.SCHEDULE_SHIP_DATE,
    a.PICK_LIST,
    a.DELIVERY,
    a.WAYBILL,
    a.SERIAL_NUMBER,
    a.ORDER_AMNT,
    a.SHIPMENT_AMNT,
    a.TO_GO_SHIPMENTS,
    a.SUPPORT_REFERENCE,
    a.ORGANIZATION_ID,
    a.COMPONENT_NUMBER,
    a.SITE_ADDRESS,
    a.cust_po_number,
    a.shipment_number,
    rma.rma_number,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || organization_id AS organization_id_KEY,
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
    ) || '-' || COALESCE(a.dw_load_id, 'NA') || '-' || COALESCE(ship.ship_filter, '1990-01-01 12:00:00') || '-' || COALESCE(rma.rma_number, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM gold_bec_dwh.FACT_OM_SHIP_BACKLOG_STG AS a, ship, rma
  WHERE
    ship.order_ship = a.order_number
    AND rma.REFERENCE_HEADER_ID() = a.header_id
    AND rma.REFERENCE_LINE_ID() = a.line_id
    AND rma.RETURN_ATTRIBUTE2() = a.serial_number
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_om_ship_backlog' AND batch_name = 'om';