DROP table IF EXISTS gold_bec_dwh.FACT_PO_SHIPMENT_DETAILS;
CREATE TABLE gold_bec_dwh.FACT_PO_SHIPMENT_DETAILS AS
(
  SELECT DISTINCT
    POLL.ORG_ID,
    POH.PO_HEADER_ID,
    POL.PO_LINE_ID,
    POLL.LINE_LOCATION_ID,
    POLL.PO_RELEASE_ID,
    POLL.SHIP_TO_ORGANIZATION_ID,
    POL.ITEM_ID AS INVENTORY_ITEM_ID,
    POH.VENDOR_ID,
    POH.VENDOR_SITE_ID,
    POH.AGENT_ID,
    POH.TERMS_ID,
    1 AS CATEGORY_SET_ID,
    POH.SEGMENT1 AS PO_NUMBER,
    POH.TYPE_LOOKUP_CODE AS PO_TYPE,
    CASE
      WHEN POH.TYPE_LOOKUP_CODE = 'STANDARD'
      THEN POH.AUTHORIZATION_STATUS
      ELSE POR.AUTHORIZATION_STATUS
    END AS AUTHORIZATION_STATUS,
    CASE
      WHEN POH.TYPE_LOOKUP_CODE = 'STANDARD'
      THEN POH.CREATION_DATE
      WHEN POH.TYPE_LOOKUP_CODE = 'BLANKET'
      THEN POR.RELEASE_DATE
      ELSE POH.CREATION_DATE
    END AS CREATION_DATE,
    POA1.AGENT_NAME AS IM_BUYER,
    POR.RELEASE_NUM,
    POR.CANCEL_FLAG AS BLNK_REL_CANCEL_FLAG,
    PLT.LINE_TYPE AS PO_LINE_TYPE,
    POL.LINE_NUM,
    MSI.SEGMENT1 AS PURCHASE_ITEM,
    POL.ITEM_ID,
    POL.ITEM_DESCRIPTION AS PURCHASE_ITEM_DESCRIPTION,
    MSI.DESCRIPTION AS ITEM_DESCRIPTION,
    POL.UNIT_MEAS_LOOKUP_CODE AS PO_UOM,
    POL.ITEM_REVISION,
    MSI.PRIMARY_UNIT_OF_MEASURE AS STD_UOM,
    POL.UNIT_PRICE,
    CIC.ITEM_COST AS STD_ITEM_COST,
    CIC.MATERIAL_COST,
    CIC.MATERIAL_OVERHEAD_COST,
    CIC.RESOURCE_COST,
    CIC.OVERHEAD_COST,
    CIC.OUTSIDE_PROCESSING_COST,
    POLL.QUANTITY AS ORDERED_QUANTITY,
    POLL.QUANTITY_RECEIVED,
    POLL.QUANTITY_BILLED,
    (
      POLL.QUANTITY - COALESCE(
        CASE
          WHEN POLL.MATCH_OPTION = 'R'
          THEN POLL.QUANTITY_RECEIVED
          ELSE POLL.QUANTITY_BILLED
        END,
        0
      )
    ) AS PO_OPEN_QTY,
    (
      POLL.QUANTITY_RECEIVED - POLL.QUANTITY_ACCEPTED - POLL.QUANTITY_REJECTED
    ) AS IQC_QUANTITY,
    (
      POLL.QUANTITY - COALESCE(POLL.QUANTITY_BILLED, 0)
    ) AS PO_OPEN_QTY_FINANCE,
    (
      POLL.QUANTITY - COALESCE(POLL.QUANTITY_RECEIVED, 0)
    ) AS PO_OPEN_QTY_RECEIVING,
    POLL.AMOUNT_RECEIVED,
    POLL.AMOUNT_BILLED,
    (
      POLL.QUANTITY * POL.UNIT_PRICE
    ) AS AMOUNT_ORDERED,
    (
      POLL.QUANTITY * POL.UNIT_PRICE - COALESCE(
        CASE
          WHEN POLL.MATCH_OPTION = 'R'
          THEN CASE
            WHEN POLL.AMOUNT_RECEIVED IS NULL
            THEN POLL.AMOUNT_BILLED
            WHEN POLL.AMOUNT_RECEIVED = 0
            THEN POLL.AMOUNT_BILLED
            ELSE POLL.AMOUNT_RECEIVED
          END
          ELSE POLL.AMOUNT_BILLED
        END,
        0
      )
    ) AS PO_OPEN_AMOUNT,
    POLL.NEED_BY_DATE,
    POLL.PROMISED_DATE,
    DATEDIFF(CURRENT_TIMESTAMP(), PROMISED_DATE) AS SUPPLY_DELAY,
    DATE_FORMAT(POLL.PROMISED_DATE, 'MON-yy') AS PROMISE_MONTH,
    POH.COMMENTS,
    COALESCE(POH.CLOSED_CODE, 'OPEN') AS PO_STATUS,
    COALESCE(POL.CLOSED_CODE, 'OPEN') AS PO_LINE_STATUS,
    COALESCE(POLL.CLOSED_CODE, 'OPEN') AS SHIPMENT_STATUS,
    CASE
      WHEN POLL.MATCH_OPTION = 'P'
      THEN 'Purchase Order'
      WHEN POLL.MATCH_OPTION = 'R'
      THEN 'Receipt'
      ELSE NULL
    END AS MATCH_OPTION,
    POLL.MATCHING_BASIS,
    CASE
      WHEN COALESCE(POLL.INSPECTION_REQUIRED_FLAG, 'N') || POLL.RECEIPT_REQUIRED_FLAG = 'YY'
      THEN '4-Way'
      WHEN COALESCE(POLL.INSPECTION_REQUIRED_FLAG, 'N') || POLL.RECEIPT_REQUIRED_FLAG = 'NY'
      THEN '3-Way'
      WHEN COALESCE(POLL.INSPECTION_REQUIRED_FLAG, 'N') || POLL.RECEIPT_REQUIRED_FLAG = 'NN'
      THEN '2-Way'
    END AS MATCH_APPROVAL_LEVEL,
    (
      POLL.PROMISED_DATE + CASE
        WHEN POH.TERMS_ID = 10002
        THEN 10
        WHEN POH.TERMS_ID = 10003
        THEN 15
        WHEN POH.TERMS_ID = 10004
        THEN 20
        WHEN POH.TERMS_ID = 10005
        THEN 25
        WHEN POH.TERMS_ID = 10006
        THEN 28
        WHEN POH.TERMS_ID = 10007
        THEN 30
        WHEN POH.TERMS_ID = 10008
        THEN 45
        WHEN POH.TERMS_ID = 10009
        THEN 60
        WHEN POH.TERMS_ID = 10010
        THEN 7
        WHEN POH.TERMS_ID = 10120
        THEN 120
        WHEN POH.TERMS_ID = 10100
        THEN 90
        ELSE 0
      END
    ) AS PAYMENT_DUE_DATE,
    DATE_FORMAT(POLL.PROMISED_DATE + CASE
      WHEN POH.TERMS_ID = 10002
      THEN 10
      WHEN POH.TERMS_ID = 10003
      THEN 15
      WHEN POH.TERMS_ID = 10004
      THEN 20
      WHEN POH.TERMS_ID = 10005
      THEN 25
      WHEN POH.TERMS_ID = 10006
      THEN 28
      WHEN POH.TERMS_ID = 10007
      THEN 30
      WHEN POH.TERMS_ID = 10008
      THEN 45
      WHEN POH.TERMS_ID = 10009
      THEN 60
      WHEN POH.TERMS_ID = 10010
      THEN 7
      WHEN POH.TERMS_ID = 10120
      THEN 120
      WHEN POH.TERMS_ID = 10100
      THEN 90
      ELSE 0
    END, 'MON-yy') AS PAYMENT_MONTH,
    POH.FOB_LOOKUP_CODE AS FOB,
    POH.FREIGHT_TERMS_LOOKUP_CODE AS FREIGHT_TERMS,
    POH.SHIP_VIA_LOOKUP_CODE AS CARRIER,
    RRH.ROUTING_NAME AS RCV_ROUTING_NAME,
    POLL.SHIPMENT_NUM,
    POLL.DROP_SHIP_FLAG,
    OOD.ORGANIZATION_NAME,
    HL.LOCATION_CODE AS SHIP_TO_LOCATION,
    COALESCE(HL.ADDRESS_LINE_1, 'NA') || ',' || COALESCE(HL.ADDRESS_LINE_2, 'NA') AS SHIP_TO_ADDRESS,
    COALESCE(POLL.VMI_FLAG, 'N') AS VMI_FLAG,
    COALESCE(POLL.CONSIGNED_FLAG, 'N') AS CVMI_FLAG,
    msi.attribute5 AS program_name,
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
    ) || '-' || COALESCE(POLL.LINE_LOCATION_ID, '0') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.PO_LINE_LOCATIONS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS POLL
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.HR_LOCATIONS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS hl
    ON POLL.SHIP_TO_LOCATION_ID = HL.LOCATION_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.PO_LINES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS POL
    ON POLL.PO_HEADER_ID = POL.PO_HEADER_ID AND POLL.PO_LINE_ID = POL.PO_LINE_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.PO_HEADERS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS poh
    ON POL.PO_HEADER_ID = POH.PO_HEADER_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.PO_DISTRIBUTIONS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS pod
    ON POLL.LINE_LOCATION_ID = POD.LINE_LOCATION_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.PO_RELEASES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS POR
    ON POLL.PO_RELEASE_ID = POR.PO_RELEASE_ID AND POLL.PO_HEADER_ID = POR.PO_HEADER_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.MTL_SYSTEM_ITEMS_B
    WHERE
      is_deleted_flg <> 'Y'
  ) AS MSI
    ON POL.ITEM_ID = MSI.INVENTORY_ITEM_ID
    AND POLL.SHIP_TO_ORGANIZATION_ID = MSI.ORGANIZATION_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.PO_LINE_TYPES_TL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS PLT
    ON POL.LINE_TYPE_ID = PLT.LINE_TYPE_ID AND PLT.`language` = 'US'
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.PO_AGENTS_V
    WHERE
      is_deleted_flg <> 'Y'
  ) AS POA1
    ON MSI.BUYER_ID = POA1.AGENT_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.CST_ITEM_COSTS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS CIC
    ON POL.ITEM_ID = CIC.INVENTORY_ITEM_ID
    AND POLL.SHIP_TO_ORGANIZATION_ID = CIC.ORGANIZATION_ID
    AND CIC.COST_TYPE_ID = 1
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.RCV_ROUTING_HEADERS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RRH
    ON POLL.RECEIVING_ROUTING_ID = RRH.ROUTING_HEADER_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM silver_bec_ods.ORG_ORGANIZATION_DEFINITIONS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS OOD
    ON POLL.SHIP_TO_ORGANIZATION_ID = OOD.ORGANIZATION_ID
  WHERE
    1 = 1
    AND (
      POL.CANCEL_FLAG = 'N' OR POL.CANCEL_FLAG IS NULL
    )
    AND (
      POH.CANCEL_FLAG = 'N' OR POH.CANCEL_FLAG IS NULL
    )
    AND (
      POLL.CANCEL_FLAG = 'N' OR POLL.CANCEL_FLAG IS NULL
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_po_shipment_details' AND batch_name = 'po';