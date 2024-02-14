TRUNCATE table gold_bec_dwh.FACT_OM_SHIPMENT;
INSERT INTO gold_bec_dwh.FACT_OM_SHIPMENT
(
  SELECT
    ORDER_HEADER_ID,
    DELIVERY_DETAIL_ID,
    MOVE_ORDER_LINE_ID,
    ORDER_LINE_ID,
    ORG_ID,
    ORGANIZATION_ID,
    INVENTORY_ITEM_ID,
    DELIVERY_NUMBER,
    DELIVERY_NAME,
    MOVE_ORDER_NUMBER,
    MO_TRANSACTION_ID,
    DATE_SCHEDULED,
    RELEASED_STATUS, /* Lookup 'PICK_STATUS' */
    TRACKING_NUMBER,
    WAYBILL,
    SHIP_METHOD_CODE, /* join with lookups dimension SHIP_METHOD */
    BILL_OF_LADDING,
    FREIGHT_CARRIER,
    SERIAL_NUMBER,
    FROM_SUB_INVENTORY,
    DELIVER_TO_SITE_USE_ID,
    DELIVER_TO_LOCATION_ID,
    DELIVER_TO_CONTACT_ID, /* Need to check */
    DELIVER_TO,
    SHIPMENT_NUMBER,
    ACCEPTED_DATE,
    ACTION_REQUIRED,
    REQUESTED_QUANTITY,
    PICKED_QUANTITY,
    DATE_REQUESTED,
    DATE_SCHEDULED AS SHIPPING_DATE,
    CREATION_DATE,
    PICK_SLIP_NUMBER,
    PICK_RELEASE_DATE,
    ORDER_TYPE_ID,
    LINE_TYPE_ID,
    DELIVERED_DATE,
    SALESREP_ID,
    ORDER_SOURCE_ID,
    SOURCE_TYPE_CODE,
    PICKABLE_FLAG, /* Added to fix UAT issue. */
    DELIVERY_STATUS, /* Added to fix UAT issue. */
    ACCOUNT_EXECUTIVE, /* Added to fix UAT issue. */
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
    ) || '-' || COALESCE(DELIVERY_DETAIL_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      WDD.SOURCE_HEADER_ID AS ORDER_HEADER_ID,
      WDD.DELIVERY_DETAIL_ID,
      WDD.MOVE_ORDER_LINE_ID,
      WDD.SOURCE_LINE_ID AS ORDER_LINE_ID,
      WDD.ORG_ID,
      WDD.ORGANIZATION_ID,
      WDD.INVENTORY_ITEM_ID,
      WND.DELIVERY_ID AS DELIVERY_NUMBER,
      WND.NAME AS DELIVERY_NAME,
      MTRH.REQUEST_NUMBER AS MOVE_ORDER_NUMBER,
      MAX(COALESCE(mmtt.transaction_temp_id, MMT.TRANSACTION_ID)) AS MO_TRANSACTION_ID,
      WDD.DATE_SCHEDULED,
      WDD.RELEASED_STATUS, /* Lookup 'PICK_STATUS' */
      WDD.TRACKING_NUMBER,
      WND.WAYBILL,
      WnD.SHIP_METHOD_CODE, /* join with lookups dimension SHIP_METHOD */
      WDI.SEQUENCE_NUMBER AS BILL_OF_LADDING,
      WC.FREIGHT_CODE AS FREIGHT_CARRIER,
      WDD.SERIAL_NUMBER AS SERIAL_NUMBER,
      WDD.SUBINVENTORY AS FROM_SUB_INVENTORY,
      WDD.DELIVER_TO_SITE_USE_ID,
      WDD.DELIVER_TO_LOCATION_ID,
      WDD.DELIVER_TO_CONTACT_ID, /* Need to check */
      COALESCE(HL.ADDRESS1, 'NA') || '-' || COALESCE(HL.ADDRESS2, 'NA') || '-' || COALESCE(HL.CITY, 'NA') || '-' || COALESCE(HL.STATE, 'NA') || '-' || COALESCE(HL.POSTAL_CODE, 'NA') || '-' || COALESCE(HL.COUNTRY, 'NA') AS DELIVER_TO,
      WDD.SHIPMENT_LINE_NUMBER AS SHIPMENT_NUMBER, /* ,MTRH.REQUEST_NUMBER MOVE_ORDER_NUMBER */ /* ,MMT.TRANSACTION_ID MO_TRANSACTION_ID */
      WND.ACCEPTED_DATE,
      CASE
        WHEN WDD.RELEASED_STATUS = 'Y'
        THEN 'Ship Confirm'
        WHEN WDD.RELEASED_STATUS = 'S'
        THEN 'Pick-Confirm/Transact Move Order'
        WHEN WDD.RELEASED_STATUS = 'C'
        THEN 'Not Applicable'
        WHEN WDD.RELEASED_STATUS = 'R'
        THEN 'Pick Release'
        ELSE 'None'
      END AS ACTION_REQUIRED,
      WDD.REQUESTED_QUANTITY,
      WDD.PICKED_QUANTITY,
      WDD.DATE_REQUESTED,
      WDD.DATE_SCHEDULED AS SHIPPING_DATE,
      WDD.CREATION_DATE,
      MAX(COALESCE(MMTT.pick_slip_number, mmt.PICK_SLIP_NUMBER, MTRL.PICK_SLIP_NUMBER)) AS PICK_SLIP_NUMBER,
      MAX(COALESCE(MMTT.creation_date, mmt.creation_date, MTRL.PICK_SLIP_DATE)) AS PICK_RELEASE_DATE,
      OOH.ORDER_TYPE_ID,
      OOL.LINE_TYPE_ID,
      WND.DELIVERED_DATE,
      OOH.SALESREP_ID,
      OOH.ORDER_SOURCE_ID,
      OOL.SOURCE_TYPE_CODE,
      WDD.PICKABLE_FLAG, /* Added to fix UAT issue. */
      CASE
        WHEN WND.STATUS_CODE = 'OP'
        THEN 'Open'
        WHEN WND.STATUS_CODE = 'CL'
        THEN 'Closed'
      END AS DELIVERY_STATUS, /* Added to fix UAT issue. */ /* ,DECODE(UPPER(OOH.ATTRIBUTE8),'INDIA', '999999', OOH.ATTRIBUTE8) ACCOUNT_EXECUTIVE--Added to fix UAT issue.	*/
      CASE
        WHEN CASE WHEN OOH.attribute8 = ' India' THEN 'INDIA' ELSE UPPER(OOH.ATTRIBUTE8) END = 'INDIA'
        THEN '999999'
        ELSE OOH.ATTRIBUTE8
      END AS ACCOUNT_EXECUTIVE /* Added to fix PROD issue */
    FROM (
      SELECT
        *
      FROM silver_bec_ods.WSH_DELIVERY_DETAILS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS WDD, (
      SELECT
        *
      FROM silver_bec_ods.WSH_DELIVERY_ASSIGNMENTS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS WDA, (
      SELECT
        *
      FROM silver_bec_ods.WSH_NEW_DELIVERIES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS WND, (
      SELECT
        *
      FROM silver_bec_ods.WSH_CARRIERS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS WC, (
      SELECT
        *
      FROM silver_bec_ods.WSH_DELIVERY_LEGS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS WDL, (
      SELECT
        *
      FROM silver_bec_ods.WSH_DOCUMENT_INSTANCES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS WDI, (
      SELECT
        *
      FROM silver_bec_ods.MTL_TXN_REQUEST_LINES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS MTRL, (
      SELECT
        *
      FROM silver_bec_ods.MTL_TXN_REQUEST_HEADERS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS MTRH, (
      SELECT
        *
      FROM silver_bec_ods.mtl_material_transactions_temp
      WHERE
        is_deleted_flg <> 'Y'
    ) AS mmtt, (
      SELECT
        *
      FROM silver_bec_ods.MTL_MATERIAL_TRANSACTIONS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS MMT, (
      SELECT
        *
      FROM silver_bec_ods.OE_ORDER_HEADERS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS OOH, (
      SELECT
        *
      FROM silver_bec_ods.OE_ORDER_LINES_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS OOL, (
      SELECT
        *
      FROM silver_bec_ods.HZ_LOCATIONS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS HL
    WHERE
      1 = 1
      AND WDD.RELEASED_STATUS <> 'D'
      AND WDD.DELIVERY_DETAIL_ID = WDA.DELIVERY_DETAIL_ID()
      AND WDA.DELIVERY_ID = WND.DELIVERY_ID()
      AND WND.CARRIER_ID = WC.CARRIER_ID()
      AND WND.DELIVERY_ID = WDL.DELIVERY_ID()
      AND WDL.delivery_leg_id = WDI.ENTITY_ID()
      AND WDI.ENTITY_NAME() = 'WSH_DELIVERY_LEGS'
      AND WDI.DOCUMENT_TYPE() = 'BOL'
      AND WDD.MOVE_ORDER_LINE_ID = MTRL.LINE_ID()
      AND MTRL.HEADER_ID = MTRH.HEADER_ID()
      AND MTRL.LINE_ID = MMT.MOVE_ORDER_LINE_ID()
      AND MTRL.LINE_ID = MMTT.MOVE_ORDER_LINE_ID()
      AND (
        MMT.TRANSACTION_TYPE_ID IS NULL
        OR MMT.TRANSACTION_TYPE_ID = 52
        OR MMT.TRANSACTION_TYPE_ID = 53
      )
      AND WDD.SOURCE_HEADER_ID = OOL.HEADER_ID
      AND WDD.SOURCE_LINE_ID = OOL.LINE_ID
      AND OOL.HEADER_ID = OOH.HEADER_ID
      AND OOL.ORG_ID = OOH.ORG_ID
      AND WDD.DELIVER_TO_LOCATION_ID = HL.LOCATION_ID()
    GROUP BY
      WDD.SOURCE_HEADER_ID,
      WDD.DELIVERY_DETAIL_ID,
      WDD.MOVE_ORDER_LINE_ID,
      WDD.SOURCE_LINE_ID,
      WDD.ORG_ID,
      WDD.ORGANIZATION_ID,
      WDD.INVENTORY_ITEM_ID,
      WND.DELIVERY_ID,
      WND.NAME,
      MTRH.REQUEST_NUMBER, /* ,MAX(NVL(mmtt.transaction_temp_id, MMT.TRANSACTION_ID))   */
      WDD.DATE_SCHEDULED,
      WDD.RELEASED_STATUS, /* Lookup 'PICK_STATUS' */
      WDD.TRACKING_NUMBER,
      WND.WAYBILL,
      WnD.SHIP_METHOD_CODE, /* join with lookups dimension SHIP_METHOD */
      WDI.SEQUENCE_NUMBER,
      WC.FREIGHT_CODE,
      WDD.SERIAL_NUMBER,
      WDD.SUBINVENTORY,
      WDD.DELIVER_TO_SITE_USE_ID,
      WDD.DELIVER_TO_LOCATION_ID,
      WDD.DELIVER_TO_CONTACT_ID, /* Need to check */
      COALESCE(HL.ADDRESS1, 'NA') || '-' || COALESCE(HL.ADDRESS2, 'NA') || '-' || COALESCE(HL.CITY, 'NA') || '-' || COALESCE(HL.STATE, 'NA') || '-' || COALESCE(HL.POSTAL_CODE, 'NA') || '-' || COALESCE(HL.COUNTRY, 'NA'),
      WDD.SHIPMENT_LINE_NUMBER, /* ,MTRH.REQUEST_NUMBER MOVE_ORDER_NUMBER */ /* ,MMT.TRANSACTION_ID MO_TRANSACTION_ID */
      WND.ACCEPTED_DATE,
      CASE
        WHEN WDD.RELEASED_STATUS = 'Y'
        THEN 'Ship Confirm'
        WHEN WDD.RELEASED_STATUS = 'S'
        THEN 'Pick-Confirm/Transact Move Order'
        WHEN WDD.RELEASED_STATUS = 'C'
        THEN 'Not Applicable'
        WHEN WDD.RELEASED_STATUS = 'R'
        THEN 'Pick Release'
        ELSE 'None'
      END,
      WDD.REQUESTED_QUANTITY,
      WDD.PICKED_QUANTITY,
      WDD.DATE_REQUESTED,
      WDD.DATE_SCHEDULED,
      WDD.CREATION_DATE, /* ,MAX(COALESCE(MMTT.pick_slip_number,mmt.PICK_SLIP_NUMBER,MTRL.PICK_SLIP_NUMBER)) */ /* ,MAX(COALESCE(MMTT.creation_date, mmt.creation_date, MTRL.PICK_SLIP_DATE))  PICK_RELEASE_DATE */
      OOH.ORDER_TYPE_ID,
      OOL.LINE_TYPE_ID,
      WND.DELIVERED_DATE,
      OOH.SALESREP_ID,
      OOH.ORDER_SOURCE_ID,
      OOL.SOURCE_TYPE_CODE,
      WDD.PICKABLE_FLAG, /* Added to fix UAT issue. */
      CASE
        WHEN WND.STATUS_CODE = 'OP'
        THEN 'Open'
        WHEN WND.STATUS_CODE = 'CL'
        THEN 'Closed'
      END, /* Added to fix UAT issue. */
      CASE
        WHEN CASE WHEN OOH.attribute8 = ' India' THEN 'INDIA' ELSE UPPER(OOH.ATTRIBUTE8) END = 'INDIA'
        THEN '999999'
        ELSE OOH.ATTRIBUTE8
      END
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_om_shipment' AND batch_name = 'om';