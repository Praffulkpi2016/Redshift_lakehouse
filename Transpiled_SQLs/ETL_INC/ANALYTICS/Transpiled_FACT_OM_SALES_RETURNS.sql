/* delete */
DELETE FROM gold_bec_dwh.FACT_OM_SALES_RETURNS
WHERE
  EXISTS(
    SELECT
      1
    FROM BEC_ODS.OE_ORDER_LINES_ALL AS OOL
    INNER JOIN BEC_ODS.OE_ORDER_HEADERS_ALL AS OOH
      ON OOL.HEADER_ID = OOH.HEADER_ID
      AND OOL.ORG_ID = OOH.ORG_ID
      AND OOH.cancelled_flag = 'N'
      AND OOL.cancelled_flag = 'N'
    INNER JOIN BEC_ODS.OE_TRANSACTION_TYPES_TL AS OTT
      ON ool.line_type_id = ott.transaction_type_id
      AND ott.NAME = 'Return Only Line'
      AND OTT.LANGUAGE = 'US'
    LEFT OUTER JOIN gold_bec_dwh.DIM_CUSTOMER_DETAILS AS SHIP
      ON OOL.SHIP_TO_ORG_ID = SHIP.SITE_USE_ID AND SHIP.SITE_USE_CODE = 'SHIP_TO'
    LEFT OUTER JOIN BEC_ODS.hz_party_sites AS HPS
      ON (
        HPS.PARTY_SITE_ID = SHIP.PARTY_SITE_ID
      )
    LEFT OUTER JOIN BEC_ODS.HZ_CUST_ACCOUNTS AS HCA
      ON OOH.SOLD_TO_ORG_ID = HCA.CUST_ACCOUNT_ID
    LEFT OUTER JOIN BEC_ODS.HZ_PARTIES AS HP
      ON HCA.PARTY_ID = HP.PARTY_ID
    INNER JOIN BEC_ODS.CS_ESTIMATE_DETAILS AS CED
      ON OOH.HEADER_ID = CED.ORDER_HEADER_ID
      AND OOH.SOURCE_DOCUMENT_ID = CED.INCIDENT_ID
      AND OOL.LINE_ID = CED.ORDER_LINE_ID
    LEFT OUTER JOIN BEC_ODS.CS_INCIDENTS_ALL_B AS INC
      ON CED.INCIDENT_ID = INC.INCIDENT_ID
    LEFT OUTER JOIN BEC_ODS.CSI_ITEM_INSTANCES AS CII
      ON INC.CUSTOMER_PRODUCT_ID = CII.INSTANCE_ID
    WHERE
      1 = 1
      AND (
        OOH.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_om_sales_returns' AND batch_name = 'om'
        )
        OR OOL.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_om_sales_returns' AND batch_name = 'om'
        )
      )
      AND fact_om_sales_returns.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(OOH.HEADER_ID, 0) || '-' || COALESCE(OOL.LINE_ID, 0) || '-' || COALESCE(OOH.ORG_ID, 0)
  );
INSERT INTO gold_bec_dwh.fact_om_sales_returns
(
  SELECT
    OOH.ORG_ID,
    OOH.ORDER_TYPE_ID,
    OOH.HEADER_ID,
    OOL.LINE_ID,
    CED.SERIAL_NUMBER,
    CII.EXTERNAL_REFERENCE AS PADID,
    INC.INCIDENT_NUMBER AS SR_NUM,
    OOL.LINE_TYPE_ID,
    OOL.SHIP_FROM_ORG_ID,
    OOH.ORDER_NUMBER,
    OOH.ORDERED_DATE,
    OOH.PRICE_LIST_ID,
    OOH.CUST_PO_NUMBER,
    SHIP.PARTY_NAME AS SHIP_TO_CUSTOMER_NAME,
    HP.PARTY_NAME AS SOLD_TO_CUSTOMER_NAME,
    SHIP.ADDRESS1,
    OOL.LINE_NUMBER || '.' || OOL.SHIPMENT_NUMBER AS LINE_NUMBER,
    OOL.ORDERED_ITEM,
    OOL.INVENTORY_ITEM_ID,
    COALESCE(OOL.unit_selling_price, 0) AS UNIT_SELLING_PRICE,
    OOL.REQUEST_DATE,
    OOL.ORDERED_QUANTITY,
    OOL.FULFILLED_QUANTITY,
    OOL.RETURN_REASON_CODE,
    OOH.FLOW_STATUS_CODE AS HDR_STATUS,
    OOL.FLOW_STATUS_CODE AS LINE_STATUS,
    OOH.OPEN_FLAG AS HDR_OPEN_FLAG,
    OOL.OPEN_FLAG AS LINE_OPE_FLAG,
    OOL.CREATION_DATE,
    OOL.LAST_UPDATE_DATE,
    OOH.CREATION_DATE AS HDR_CREATION_DATE,
    OOH.LAST_UPDATE_DATE AS HDR_UPDATE_DATE,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || OOH.HEADER_ID AS HEADER_ID_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || OOL.LINE_TYPE_ID AS LINE_TYPE_ID_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || OOH.ORG_ID AS ORG_ID_key,
    ool.shipped_quantity,
    HPS.party_site_name,
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
    ) || '-' || COALESCE(OOH.HEADER_ID, 0) || '-' || COALESCE(OOL.LINE_ID, 0) || '-' || COALESCE(OOH.ORG_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM BEC_ODS.OE_ORDER_LINES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS OOL
  INNER JOIN (
    SELECT
      *
    FROM BEC_ODS.OE_ORDER_HEADERS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS OOH
    ON OOL.HEADER_ID = OOH.HEADER_ID
    AND OOL.ORG_ID = OOH.ORG_ID
    AND OOH.cancelled_flag = 'N'
    AND OOL.cancelled_flag = 'N'
  INNER JOIN (
    SELECT
      *
    FROM BEC_ODS.OE_TRANSACTION_TYPES_TL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS OTT
    ON ool.line_type_id = ott.transaction_type_id
    AND ott.NAME = 'Return Only Line'
    AND OTT.LANGUAGE = 'US'
  LEFT OUTER JOIN gold_bec_dwh.DIM_CUSTOMER_DETAILS AS SHIP
    ON OOL.SHIP_TO_ORG_ID = SHIP.SITE_USE_ID AND SHIP.SITE_USE_CODE = 'SHIP_TO'
  LEFT OUTER JOIN BEC_ODS.hz_party_sites AS HPS
    ON (
      HPS.PARTY_SITE_ID = SHIP.PARTY_SITE_ID
    )
  LEFT OUTER JOIN (
    SELECT
      *
    FROM BEC_ODS.HZ_CUST_ACCOUNTS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS HCA
    ON OOH.SOLD_TO_ORG_ID = HCA.CUST_ACCOUNT_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM BEC_ODS.HZ_PARTIES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS HP
    ON HCA.PARTY_ID = HP.PARTY_ID
  INNER JOIN (
    SELECT
      *
    FROM BEC_ODS.CS_ESTIMATE_DETAILS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS CED
    ON OOH.HEADER_ID = CED.ORDER_HEADER_ID
    AND OOH.SOURCE_DOCUMENT_ID = CED.INCIDENT_ID
    AND OOL.LINE_ID = CED.ORDER_LINE_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM BEC_ODS.CS_INCIDENTS_ALL_B
    WHERE
      is_deleted_flg <> 'Y'
  ) AS INC
    ON CED.INCIDENT_ID = INC.INCIDENT_ID
  LEFT OUTER JOIN (
    SELECT
      *
    FROM BEC_ODS.CSI_ITEM_INSTANCES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS CII
    ON INC.CUSTOMER_PRODUCT_ID = CII.INSTANCE_ID
  WHERE
    1 = 1
    AND (
      OOH.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_om_sales_returns' AND batch_name = 'om'
      )
      OR OOL.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'fact_om_sales_returns' AND batch_name = 'om'
      )
    )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_om_sales_returns' AND batch_name = 'om';