DROP TABLE IF EXISTS BEC_DWH.FACT_ASN_SHIPMENT;
CREATE TABLE BEC_DWH.FACT_ASN_SHIPMENT AS
(
  SELECT
    SHP.SHIPMENT_HEADER_ID,
    SHP.SHIPMENT_LINE_ID,
    SHP.PO_HEADER_ID,
    SHP.PO_LINE_ID,
    SHP.PO_NUMBER,
    SHP.PO_STATUS_LOOKUP_CODE,
    SHP.ITEM_ID,
    SHP.AGENT_ID,
    SHP.PO_DATE,
    SHP.LOCATOR_ID,
    SHP.PO_LINE_QUANTITY,
    SHP.LINE_TYPE_ID,
    SHP.ITEM_CATEGORY_ID,
    SHP.ORG_ID,
    SHP.vendor_id,
    SHP.vendor_site_id,
    SHP.SHIPMENT_HEADER_ID_KEY,
    SHP.SHIPMENT_LINE_ID_KEY,
    SHP.PO_HEADER_ID_KEY,
    SHP.PO_LINE_ID_KEY,
    SHP.ITEM_ID_KEY,
    SHP.AGENT_ID_KEY,
    SHP.ORG_ID_KEY,
    SHP.vendor_id_key,
    SHP.vendor_site_id_key,
    SHP.LOCATOR_ID_key,
    SHP.ITEM_CATEGORY_ID_key,
    SHP.LINE_TYPE_ID_KEY,
    SHP.ASN_TYPE,
    SHP.VENDOR_NAME,
    SHP.RSH_PACKING_SLIP,
    SHP.SHIPMENT_NUM,
    SHP.ASN_LINE,
    SHP.PO_SHIPMENT_LINE,
    SHP.WAYBILL_AIRBILL_NUM,
    SHP.RECEIPT_NUM,
    SHP.SHIPMENT_LINE_STATUS_CODE,
    SHP.CREATION_DATE,
    SHP.SHIPPED_DATE,
    SHP.EXPECTED_RECEIPT_DATE,
    SHP.RSH_COMMENTS,
    SHP.BILL_OF_LADING,
    SHP.FREIGHT_CARRIER_CODE,
    SHP.NUM_OF_CONTAINERS,
    SHP.PART_NUMBER,
    SHP.ITEM_DESCRIPTION,
    SHP.ITEM_REVISION,
    SHP.SOURCE_DOCUMENT_TYPE,
    SHP.ORDER_NUM,
    SHP.ORDER_LINE_NUM,
    SHP.LINE_NUM,
    SHP.RELEASE_NUM,
    SHP.NEED_BY_DATE,
    SHP.QUANTITY_SHIPPED,
    SHP.QUANTITY_RECEIVED,
    SHP.UNIT_OF_MEASURE,
    SHP.PRIMARY_UNIT_OF_MEASURE,
    SHP.ROUTING_NAME,
    SHP.RECEIPT_SOURCE_CODE,
    SHP.TO_ORGANIZATION_ID,
    SHP.ORGANIZATION_NAME,
    SHP.CONTAINER_NUM,
    SHP.LOT_NUM,
    SHP.SERIAL_NUM,
    SHP.LOT_QTY,
    SHP.SERIAL_NUMBER_CONTROL,
    SHP.CURRENT_STATUS,
    SHP.LOT_CONTROL,
    SHP.PAY_ON_CODE,
    SHP.TERM_NAME,
    SHP.UNIT_PRICE,
    SHP.SHIPMENT_AMOUNT,
    SHP.PROMISED_DATE,
    SHP.COMMIT_DATE,
    SHP.LAST_UPDATE_DATE,
    SHP.CVMI_FLAG,
    SHP.ITEM_COST,
    SHP.MATERIAL_COST,
    SHP.PO_DESCRIPTION,
    SHP.AUTHORIZATION_STATUS,
    SHP.PO_UNIT_PRICE,
    SHP.QUANTITY_ORDERED,
    SHP.VMI_FLAG,
    SHP.REQUISITION_NUMBER,
    SHP.CHARGE_ACCOUNT_ID,
    SHP.TO_SUBINVENTORY,
    SHP.DELIVER_TO_PERSON,
    SHP.type_lookup_code,
    (
      SELECT
        mc.segment1 || '.' || mc.segment2
      FROM silver_bec_ods.mtl_categories_b AS mc, silver_bec_ods.mtl_item_categories AS mic
      WHERE
        mic.inventory_item_id = SHP.ITEM_ID
        AND mic.organization_id = SHP.TO_ORGANIZATION_ID
        AND mic.category_id = mc.category_id
        AND mic.category_set_id = 1
    ) AS CATEGORY,
    CASE WHEN SHP.SERIAL_NUM IS NULL THEN NULL ELSE 1 END AS SERIAL_NUM_QTY,
    SUBSTRING(SHP.TERM_NAME, 1, 3) AS payment_term,
    PAYMENT_dAYS,
    DATE_ADD(Payment_date, PAYMENT_dAYS) AS payment_due_date, /* Payment_date+PAYMENT_dAYS as payment_due_date, */
    shp.wip_entity_id,
    'N' AS IS_DELETED_FLG,
    SHP.SOURCE_APP_ID,
    SHP.DW_LOAD_ID,
    SHP.DW_INSERT_DATE,
    SHP.DW_UPDATE_DATE
  FROM (
    SELECT
      STG.SHIPMENT_HEADER_ID,
      STG.SHIPMENT_LINE_ID,
      STG.PO_HEADER_ID,
      POL.PO_LINE_ID,
      POH.SEGMENT1 AS PO_NUMBER,
      POH.STATUS_LOOKUP_CODE AS PO_STATUS_LOOKUP_CODE,
      STG.ITEM_ID,
      POH.AGENT_ID,
      POH.CREATION_DATE AS PO_DATE,
      STG.LOCATOR_ID,
      STG.ORG_ID,
      STG.vendor_id,
      POH.vendor_site_id,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) || '-' || STG.SHIPMENT_HEADER_ID AS SHIPMENT_HEADER_ID_KEY,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) || '-' || STG.SHIPMENT_LINE_ID AS SHIPMENT_LINE_ID_KEY,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) || '-' || STG.PO_HEADER_ID AS PO_HEADER_ID_KEY,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) || '-' || POL.PO_LINE_ID AS PO_LINE_ID_KEY,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) || '-' || STG.ITEM_ID AS ITEM_ID_KEY,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) || '-' || POH.AGENT_ID AS AGENT_ID_KEY,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) || '-' || STG.ORG_ID AS ORG_ID_KEY,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) || '-' || STG.vendor_id AS vendor_id_key,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) || '-' || POH.vendor_site_id AS vendor_site_id_key,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) || '-' || STG.LOCATOR_ID AS LOCATOR_ID_key,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) || '-' || STG.ITEM_CATEGORY_ID AS ITEM_CATEGORY_ID_key,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) || '-' || POL.LINE_TYPE_ID AS LINE_TYPE_ID_KEY,
      POL.QUANTITY AS PO_LINE_QUANTITY,
      POL.LINE_TYPE_ID,
      STG.ITEM_CATEGORY_ID,
      STG.ASN_TYPE,
      STG.VENDOR_NAME,
      STG.RSH_PACKING_SLIP,
      STG.SHIPMENT_NUM,
      STG.LINE_NUM AS ASN_LINE,
      POLL.SHIPMENT_NUM AS PO_SHIPMENT_LINE,
      STG.WAYBILL_AIRBILL_NUM,
      STG.RECEIPT_NUM,
      STG.SHIPMENT_LINE_STATUS_CODE,
      STG.CREATION_DATE,
      STG.SHIPPED_DATE,
      STG.EXPECTED_RECEIPT_DATE,
      STG.RSH_COMMENTS,
      STG.BILL_OF_LADING,
      STG.FREIGHT_CARRIER_CODE,
      STG.NUM_OF_CONTAINERS,
      MSI.SEGMENT1 AS PART_NUMBER,
      STG.ITEM_DESCRIPTION,
      STG.ITEM_REVISION,
      STG.SOURCE_DOCUMENT_TYPE,
      STG.ORDER_NUM,
      STG.ORDER_LINE_NUM,
      STG.LINE_NUM,
      STG.RELEASE_NUM,
      STG.NEED_BY_DATE,
      STG.QUANTITY_SHIPPED AS QUANTITY_SHIPPED,
      STG.QUANTITY_RECEIVED,
      STG.UNIT_OF_MEASURE,
      STG.PRIMARY_UNIT_OF_MEASURE,
      STG.ROUTING_NAME,
      STG.RECEIPT_SOURCE_CODE,
      STG.TO_ORGANIZATION_ID,
      OOD.ORGANIZATION_NAME,
      COALESCE((
        STG.ORDER_NUM
      ), '0') || '(' || COALESCE((
        STG.RELEASE_NUM
      ), '0') || ')(' || COALESCE((
        STG.LINE_NUM
      ), '0') || ')(' || COALESCE((
        POLL.SHIPMENT_NUM
      ), '0') || ')' AS ORDER_TEXT,
      COALESCE(STG.CONTAINER_NUM, STG.RSH_COMMENTS) AS CONTAINER_NUM,
      RCVLS.LOT_NUM,
      RSS.SERIAL_NUM,
      RCVLS.QUANTITY AS LOT_QTY,
      CASE
        WHEN MSI.SERIAL_NUMBER_CONTROL_CODE = 5
        THEN 'AT RECEIPT'
        WHEN MSI.SERIAL_NUMBER_CONTROL_CODE = 1
        THEN 'NO CONTROL'
      END AS SERIAL_NUMBER_CONTROL,
      (
        SELECT
          CASE
            WHEN CURRENT_STATUS = 1
            THEN 'DEFINED BUT NOT USED'
            WHEN CURRENT_STATUS = 3
            THEN 'RESIDES IN STORES'
            WHEN CURRENT_STATUS = 4
            THEN 'ISSUED OUT OF STORES'
            WHEN CURRENT_STATUS = 5
            THEN 'RESIDES IN INTRANSIT'
            WHEN CURRENT_STATUS = 7
            THEN 'RESIDES IN RECEIVING'
            WHEN CURRENT_STATUS = 8
            THEN 'RESIDES IN WIP'
          END AS CURRENT_STATUS
        FROM BEC_ODS.MTL_SERIAL_NUMBERS AS MSN
        WHERE
          STG.ITEM_ID = MSN.INVENTORY_ITEM_ID
          AND STG.TO_ORGANIZATION_ID = MSN.CURRENT_ORGANIZATION_ID
          AND RSS.SERIAL_NUM = MSN.SERIAL_NUMBER
      ) AS CURRENT_STATUS,
      CASE
        WHEN MSI.LOT_CONTROL_CODE = 1
        THEN 'NO CONTROL'
        WHEN MSI.LOT_CONTROL_CODE = 2
        THEN 'FULL CONTROL'
      END AS LOT_CONTROL,
      POH.PAY_ON_CODE,
      AT.NAME AS TERM_NAME,
      POL.UNIT_PRICE,
      CASE
        WHEN RSS.SERIAL_NUM IS NULL
        THEN (
          POL.UNIT_PRICE * STG.QUANTITY_SHIPPED
        )
        ELSE (
          POL.UNIT_PRICE * 1
        )
      END AS SHIPMENT_AMOUNT,
      CAST(CASE
        WHEN SUBSTRING(AT.NAME, 1, 3) = 'NET'
        THEN CAST(SUBSTRING(AT.NAME, 4, LENGTH(AT.NAME)) AS INT)
        ELSE 0
      END AS INT) AS PAYMENT_dAYS,
      CASE
        WHEN POH.PAY_ON_CODE = 'RECEIPT'
        THEN STG.EXPECTED_RECEIPT_DATE
        ELSE STG.SHIPPED_DATE
      END AS Payment_date,
      POLL.PROMISED_DATE,
      POLL.ATTRIBUTE10 AS COMMIT_DATE,
      STG.LAST_UPDATE_DATE,
      COALESCE(POLL.CONSIGNED_FLAG, 'N') AS CVMI_FLAG,
      CST.ITEM_COST,
      CST.MATERIAL_COST,
      POH.COMMENTS AS PO_DESCRIPTION,
      POH.AUTHORIZATION_STATUS,
      POL.UNIT_PRICE AS PO_UNIT_PRICE,
      POLL.QUANTITY AS QUANTITY_ORDERED,
      COALESCE(poll.VMI_FLAG, 'N') AS VMI_FLAG,
      REQ.REQUISITION_NUMBER,
      STG.CHARGE_ACCOUNT_ID,
      STG.TO_SUBINVENTORY,
      STG.DELIVER_TO_PERSON,
      Poh.type_lookup_code,
      req.wip_entity_id,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) AS SOURCE_APP_ID,
      (
        SELECT
          SYSTEM_ID
        FROM BEC_ETL_CTRL.ETLSOURCEAPPID
        WHERE
          SOURCE_SYSTEM = 'EBS'
      ) || '-' || COALESCE(STG.PO_HEADER_ID, 0) || '-' || COALESCE(POL.PO_LINE_ID, 0) || '-' || COALESCE(RCVLS.LOT_NUM, '0') || '-' || COALESCE(STG.SHIPMENT_HEADER_ID, 0) || '-' || COALESCE(STG.SHIPMENT_LINE_ID, '0') || '-' || COALESCE(REQ.REQUISITION_NUMBER, '0') || '-' || COALESCE(RSS.SERIAL_NUM, 'NA') AS DW_LOAD_ID,
      CURRENT_TIMESTAMP() AS DW_INSERT_DATE,
      CURRENT_TIMESTAMP() AS DW_UPDATE_DATE
    FROM (
      SELECT
        *
      FROM BEC_DWH.FACT_PO_SHIPMENT_STG
      WHERE
        is_deleted_flg <> 'Y'
    ) AS STG, (
      SELECT
        *
      FROM silver_bec_ods.MTL_SYSTEM_ITEMS_B
      WHERE
        is_deleted_flg <> 'Y'
    ) AS MSI, (
      SELECT
        *
      FROM silver_bec_ods.PO_LINE_LOCATIONS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS POLL, (
      SELECT
        *
      FROM silver_bec_ods.ORG_ORGANIZATION_DEFINITIONS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS OOD, (
      SELECT
        *
      FROM silver_bec_ods.RCV_SERIALS_SUPPLY
      WHERE
        is_deleted_flg <> 'Y'
    ) AS RSS, (
      SELECT
        *
      FROM silver_bec_ods.RCV_LOTS_SUPPLY
      WHERE
        is_deleted_flg <> 'Y'
    ) AS RCVLS, (
      SELECT
        *
      FROM silver_bec_ods.PO_HEADERS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS POH, (
      SELECT
        *
      FROM silver_bec_ods.AP_TERMS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS at, (
      SELECT
        *
      FROM silver_bec_ods.PO_LINES_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS POL, (
      SELECT DISTINCT
        prh.segment1 AS REQUISITION_NUMBER,
        POD.PO_HEADER_ID,
        POD.PO_LINE_ID,
        prl.wip_entity_id
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
        FROM silver_bec_ods.po_req_distributions_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS prd, (
        SELECT
          *
        FROM silver_bec_ods.po_distributions_all
        WHERE
          is_deleted_flg <> 'Y'
      ) AS pod
      WHERE
        1 = 1
        AND prh.interface_source_code = 'WIP'
        AND prh.requisition_header_id = prl.requisition_header_id
        AND prl.requisition_line_id = prd.requisition_line_id
        AND prd.distribution_id = pod.req_distribution_id
    ) AS REQ, (
      SELECT
        *
      FROM silver_bec_ods.CST_ITEM_COSTS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS CST
    WHERE
      1 = 1
      AND STG.RECEIPT_SOURCE_CODE = 'VENDOR'
      AND STG.PO_HEADER_ID = POH.PO_HEADER_ID
      AND POH.TERMS_ID = AT.TERM_ID()
      AND POH.PO_HEADER_ID = POL.PO_HEADER_ID
      AND POH.PO_HEADER_ID = POLL.PO_HEADER_ID
      AND POL.PO_LINE_ID = POLL.PO_LINE_ID
      AND COALESCE(STG.QUANTITY_SHIPPED, 0) <> COALESCE(STG.QUANTITY_RECEIVED, 0)
      AND STG.ITEM_ID = MSI.INVENTORY_ITEM_ID()
      AND STG.TO_ORGANIZATION_ID = MSI.ORGANIZATION_ID()
      AND STG.PO_LINE_LOCATION_ID = POLL.LINE_LOCATION_ID()
      AND STG.SHIPMENT_LINE_ID = RSS.SHIPMENT_LINE_ID()
      AND STG.SHIPMENT_LINE_ID = RCVLS.SHIPMENT_LINE_ID()
      AND STG.SHIPMENT_LINE_STATUS_CODE <> 'FULLY RECEIVED'
      AND STG.TO_ORGANIZATION_ID = OOD.ORGANIZATION_ID
      AND MSI.INVENTORY_ITEM_ID = CST.INVENTORY_ITEM_ID()
      AND MSI.ORGANIZATION_ID = CST.ORGANIZATION_ID()
      AND POLL.PO_HEADER_ID = REQ.PO_HEADER_ID()
      AND POLL.PO_LINE_ID = REQ.PO_LINE_ID()
      AND CST.ORGANIZATION_ID() = 1
      AND (
        NOT EXISTS(
          SELECT
            ' '
          FROM silver_bec_ods.rcv_transactions_interface AS rti
          WHERE
            is_deleted_flg <> 'Y'
            AND rti.shipment_header_id = STG.shipment_header_id
            AND rti.shipment_line_id = STG.shipment_line_id
        )
      )
  ) AS SHP
);
UPDATE BEC_ETL_CTRL.BATCH_DW_INFO SET LOAD_TYPE = 'I', LAST_REFRESH_DATE = CURRENT_TIMESTAMP()
WHERE
  DW_TABLE_NAME = 'fact_asn_shipment' AND BATCH_NAME = 'po';