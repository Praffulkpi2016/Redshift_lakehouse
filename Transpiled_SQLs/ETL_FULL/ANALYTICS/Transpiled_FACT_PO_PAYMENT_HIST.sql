DROP table IF EXISTS gold_bec_dwh.FACT_PO_PAYMENT_HIST;
CREATE TABLE gold_bec_dwh.FACT_PO_PAYMENT_HIST AS
(
  SELECT
    POF.ORG_ID,
    POF.PO_HEADER_ID,
    POF.PO_LINE_ID,
    POF.ITEM_ID,
    POF.PO_RELEASE_ID,
    POF.LINE_LOCATION_ID,
    POF.PO_TYPE,
    POF.PO_NUMBER,
    POF.AGENT_ID,
    POF.PO_RELEASE_NUMBER,
    POF.PO_DATE,
    POF.PO_LINE_NUM,
    POF.LINE_QUANTITY,
    POF.PO_UNIT_OF_MEASURE,
    POF.NEED_BY_DATE,
    POF.PROMISED_DATE,
    POF.RCV_QUANTITY_RECEIVED,
    POF.PO_UNIT_PRICE,
    POF.EXTENDED_PO_RCV_PRICE,
    POF.RECEIPT_NUMBER,
    POF.RECEIPT_DATE,
    POF.RECEIPT_ENTERED_AS_DATE,
    POF.VENDOR_ID,
    POF.VENDOR_SITE_ID,
    POF.CLOSED_CODE,
    POF.AUTHORIZATION_STATUS,
    POF.PO_CURRENCY_CODE,
    POF.SHIPMENT_HEADER_ID,
    POF.SHIPMENT_LINE_ID,
    POF.LINE_TYPE,
    POF.PO_HEADER_CANCEL,
    POF.PO_LINE_CANCEL,
    POF.PO_SHIPMENT_CANCEL,
    POF.CODE_COMBINATION_ID,
    POF.CVMI_FLAG,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || pof.ORG_ID AS ORG_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || pof.ITEM_ID AS ITEM_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || pof.AGENT_ID AS AGENT_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || pof.VENDOR_ID AS VENDOR_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || pof.VENDOR_SITE_ID AS VENDOR_SITE_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || pof.CODE_COMBINATION_ID AS GL_ACCOUNT_ID_KEY,
    'N' AS IS_DELETED_FLG,
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
    ) || '-' || COALESCE(POF.PO_HEADER_ID, 0) || '-' || COALESCE(POF.PO_LINE_ID, 0) || '-' || COALESCE(POF.LINE_LOCATION_ID, 0) || '-' || COALESCE(POF.SHIPMENT_LINE_ID, 0) || '-' || COALESCE(POF.CODE_COMBINATION_ID, 0) || '-' || COALESCE(POF.RECEIPT_ENTERED_AS_DATE, '1900-01-01') || '-' || COALESCE(POF.receipt_number, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      POH.ORG_ID,
      POH.PO_HEADER_ID,
      POL.PO_LINE_ID,
      POL.ITEM_ID,
      PORL.PO_RELEASE_ID,
      POLL.LINE_LOCATION_ID,
      POH.TYPE_LOOKUP_CODE AS PO_TYPE,
      POH.SEGMENT1 AS PO_NUMBER,
      POH.AGENT_ID,
      PORL.RELEASE_NUM AS PO_RELEASE_NUMBER,
      PORL.RELEASE_DATE AS PO_DATE,
      POL.LINE_NUM AS PO_LINE_NUM,
      POL.ITEM_DESCRIPTION AS ITEM_DESCRIPTION,
      POLL.QUANTITY AS LINE_QUANTITY,
      POL.UNIT_MEAS_LOOKUP_CODE AS PO_UNIT_OF_MEASURE,
      POLL.NEED_BY_DATE,
      POLL.PROMISED_DATE,
      RSL.QUANTITY_RECEIVED AS RCV_QUANTITY_RECEIVED,
      ROUND(POL.UNIT_PRICE, 2) AS PO_UNIT_PRICE,
      ROUND(POL.UNIT_PRICE * RSL.QUANTITY_RECEIVED, 2) AS EXTENDED_PO_RCV_PRICE,
      RSH.RECEIPT_NUM AS RECEIPT_NUMBER,
      RSH.CREATION_DATE AS RECEIPT_DATE,
      RT.TRANSACTION_DATE AS RECEIPT_ENTERED_AS_DATE,
      POH.VENDOR_ID,
      POH.VENDOR_SITE_ID,
      POL.CLOSED_CODE,
      PORL.AUTHORIZATION_STATUS,
      POH.CURRENCY_CODE AS PO_CURRENCY_CODE,
      RSL.SHIPMENT_HEADER_ID,
      RSL.SHIPMENT_LINE_ID,
      PLT.LINE_TYPE,
      COALESCE(POH.CANCEL_FLAG, 'N') AS PO_HEADER_CANCEL,
      COALESCE(POL.CANCEL_FLAG, 'N') AS PO_LINE_CANCEL,
      COALESCE(POLL.CANCEL_FLAG, 'N') AS PO_SHIPMENT_CANCEL,
      POD.CODE_COMBINATION_ID,
      COALESCE(POLL.CONSIGNED_FLAG, 'N') AS CVMI_FLAG
    FROM (
      SELECT
        *
      FROM silver_bec_ods.PO_HEADERS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS POH, (
      SELECT
        *
      FROM silver_bec_ods.PO_LINES_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS POL, (
      SELECT
        *
      FROM silver_bec_ods.PO_LINE_LOCATIONS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS POLL, (
      SELECT
        *
      FROM silver_bec_ods.RCV_SHIPMENT_LINES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS RSL, (
      SELECT
        *
      FROM silver_bec_ods.RCV_SHIPMENT_HEADERS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS RSH, (
      SELECT
        *
      FROM silver_bec_ods.RCV_TRANSACTIONS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS RT, (
      SELECT
        *
      FROM silver_bec_ods.PO_LINE_TYPES_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS PLT, (
      SELECT
        *
      FROM silver_bec_ods.PO_RELEASES_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS PORL, (
      SELECT
        *
      FROM silver_bec_ods.PO_DISTRIBUTIONS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS POD
    WHERE
      1 = 1
      AND POH.PO_HEADER_ID = POL.PO_HEADER_ID()
      AND POL.PO_LINE_ID = POLL.PO_LINE_ID()
      AND POLL.PO_LINE_ID = RSL.PO_LINE_ID()
      AND POLL.LINE_LOCATION_ID = RSL.PO_LINE_LOCATION_ID()
      AND RSL.SHIPMENT_HEADER_ID = RSH.SHIPMENT_HEADER_ID()
      AND RSL.SHIPMENT_LINE_ID = RT.SHIPMENT_LINE_ID()
      AND RSL.PO_RELEASE_ID = RT.PO_RELEASE_ID()
      AND RSL.PO_LINE_LOCATION_ID = RT.PO_LINE_LOCATION_ID()
      AND RT.DESTINATION_TYPE_CODE() = 'INVENTORY'
      AND POLL.PO_RELEASE_ID = PORL.PO_RELEASE_ID()
      AND POL.LINE_TYPE_ID = PLT.LINE_TYPE_ID()
      AND POH.TYPE_LOOKUP_CODE = 'BLANKET'
      AND POLL.LINE_LOCATION_ID = POD.LINE_LOCATION_ID()
      AND POLL.PO_RELEASE_ID = POD.PO_RELEASE_ID()
      AND COALESCE(plt.language, 'US') = 'US'
    UNION
    SELECT
      POH.ORG_ID,
      POH.PO_HEADER_ID,
      POL.PO_LINE_ID,
      POL.ITEM_ID,
      NULL AS PO_RELEASE_ID,
      POLL.LINE_LOCATION_ID,
      POH.TYPE_LOOKUP_CODE AS PO_TYPE,
      POH.SEGMENT1 AS PO_NUMBER,
      POH.AGENT_ID,
      NULL AS PO_RELEASE_NUMBER,
      POH.CREATION_DATE AS PO_DATE,
      POL.LINE_NUM AS PO_LINE_NUM,
      POL.ITEM_DESCRIPTION AS ITEM_DESCRIPTION,
      POL.QUANTITY AS LINE_QUANTITY,
      POL.UNIT_MEAS_LOOKUP_CODE AS PO_UNIT_OF_MEASURE,
      POLL.NEED_BY_DATE,
      POLL.PROMISED_DATE,
      RSL.QUANTITY_RECEIVED AS RCV_QUANTITY_RECEIVED,
      ROUND(POL.UNIT_PRICE, 2) AS PO_UNIT_PRICE,
      ROUND(POL.UNIT_PRICE * RSL.QUANTITY_RECEIVED, 2) AS EXTENDED_PO_RCV_PRICE,
      RSH.RECEIPT_NUM AS RECEIPT_NUMBER,
      RSH.CREATION_DATE AS RECEIPT_DATE,
      RT.TRANSACTION_DATE AS RECEIPT_ENTERED_AS_DATE,
      POH.VENDOR_ID,
      POH.VENDOR_SITE_ID,
      POL.CLOSED_CODE,
      POH.AUTHORIZATION_STATUS,
      POH.CURRENCY_CODE AS PO_CURRENCY_CODE,
      RSL.SHIPMENT_HEADER_ID,
      RSL.SHIPMENT_LINE_ID,
      PLT.LINE_TYPE,
      COALESCE(POH.CANCEL_FLAG, 'N') AS PO_HEADER_CANCEL,
      COALESCE(POL.CANCEL_FLAG, 'N') AS PO_LINE_CANCEL,
      COALESCE(POLL.CANCEL_FLAG, 'N') AS PO_SHIPMENT_CANCEL,
      POD.CODE_COMBINATION_ID,
      COALESCE(POLL.CONSIGNED_FLAG, 'N') AS CVMI_FLAG
    FROM (
      SELECT
        *
      FROM silver_bec_ods.PO_HEADERS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS POH, (
      SELECT
        *
      FROM silver_bec_ods.PO_LINES_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS POL, (
      SELECT
        *
      FROM silver_bec_ods.PO_LINE_LOCATIONS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS POLL, (
      SELECT
        *
      FROM silver_bec_ods.RCV_SHIPMENT_LINES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS RSL, (
      SELECT
        *
      FROM silver_bec_ods.RCV_TRANSACTIONS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS RT, (
      SELECT
        *
      FROM silver_bec_ods.RCV_SHIPMENT_HEADERS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS RSH, (
      SELECT
        *
      FROM silver_bec_ods.PO_LINE_TYPES_tl
      WHERE
        is_deleted_flg <> 'Y'
    ) AS PLT, (
      SELECT
        *
      FROM silver_bec_ods.PO_DISTRIBUTIONS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS POD
    WHERE
      1 = 1
      AND POH.PO_HEADER_ID = POL.PO_HEADER_ID()
      AND POL.PO_LINE_ID = POLL.PO_LINE_ID()
      AND POLL.PO_LINE_ID = RSL.PO_LINE_ID()
      AND POLL.LINE_LOCATION_ID = RSL.PO_LINE_LOCATION_ID()
      AND RSL.SHIPMENT_HEADER_ID = RSH.SHIPMENT_HEADER_ID()
      AND RSL.SHIPMENT_LINE_ID = RT.SHIPMENT_LINE_ID()
      AND RSL.PO_LINE_LOCATION_ID = RT.PO_LINE_LOCATION_ID()
      AND RSL.PO_LINE_ID = RT.PO_LINE_ID()
      AND RT.DESTINATION_TYPE_CODE() = 'INVENTORY'
      AND POL.LINE_TYPE_ID = PLT.LINE_TYPE_ID()
      AND POH.TYPE_LOOKUP_CODE = 'STANDARD'
      AND POLL.LINE_LOCATION_ID = POD.LINE_LOCATION_ID()
      AND COALESCE(plt.language, 'US') = 'US'
  ) AS POF
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po';