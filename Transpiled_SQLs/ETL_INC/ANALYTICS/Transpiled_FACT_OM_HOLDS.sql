DELETE FROM gold_bec_dwh.FACT_OM_HOLDS
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.OE_ORDER_HOLDS_ALL AS OHL
    INNER JOIN silver_bec_ods.OE_ORDER_HEADERS_ALL AS OOHL
      ON OHL.HEADER_ID = OOHL.HEADER_ID
    LEFT OUTER JOIN silver_bec_ods.OE_ORDER_LINES_ALL AS OOLA
      ON OHL.LINE_ID = OOLA.LINE_ID
      AND OOHL.HEADER_ID = OOLA.HEADER_ID
      AND OOHL.ORG_ID = OOLA.ORG_ID
    LEFT OUTER JOIN silver_bec_ods.OE_TRANSACTION_TYPES_TL AS OTTT_H
      ON OOHL.ORDER_TYPE_ID = OTTT_H.TRANSACTION_TYPE_ID AND OTTT_H.LANGUAGE = 'US'
    LEFT OUTER JOIN gold_bec_dwh.DIM_CUSTOMER_DETAILS AS BCDV
      ON OOHL.SHIP_TO_ORG_ID = BCDV.SITE_USE_ID AND OOHL.SOLD_TO_ORG_ID = BCDV.CUST_ACCOUNT_ID
    LEFT OUTER JOIN silver_bec_ods.OE_HOLD_SOURCES_ALL AS OHS
      ON OHL.HOLD_SOURCE_ID = OHS.HOLD_SOURCE_ID
    LEFT OUTER JOIN silver_bec_ods.OE_HOLD_RELEASES AS OHR
      ON OHL.HOLD_RELEASE_ID = OHR.HOLD_RELEASE_ID
    LEFT OUTER JOIN silver_bec_ods.OE_HOLD_DEFINITIONS AS OHD
      ON OHS.HOLD_ID = OHD.HOLD_ID
    LEFT OUTER JOIN silver_bec_ods.OE_HOLD_AUTHORIZATIONS AS OHA
      ON OHD.HOLD_ID = OHA.HOLD_ID AND OHA.AUTHORIZED_ACTION_CODE = 'REMOVE'
    LEFT OUTER JOIN silver_bec_ods.FND_RESPONSIBILITY_TL AS FRTL
      ON OHA.RESPONSIBILITY_ID = FRTL.RESPONSIBILITY_ID AND FRTL.LANGUAGE = 'US'
    LEFT OUTER JOIN silver_bec_ods.FND_LOOKUP_VALUES AS HT
      ON OHD.TYPE_CODE = HT.LOOKUP_CODE AND HT.LOOKUP_TYPE = 'HOLD_TYPE' AND HT.language = 'US'
    WHERE
      1 = 1
      AND (
        OOHL.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_om_holds' AND batch_name = 'om'
        )
        OR OHL.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_om_holds' AND batch_name = 'om'
        )
      )
      AND FACT_OM_HOLDS.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(OOHL.HEADER_ID, 0) || '-' || COALESCE(OHL.ORDER_HOLD_ID, 0)
  );
INSERT INTO gold_bec_dwh.FACT_OM_HOLDS
(
  SELECT
    HEADER_ID,
    LINE_TYPE_ID,
    ORDER_HOLD_ID,
    HOLD_CREATED_BY,
    RELEASE_CREATED_BY, /* ,RESPONSIBILITY_ID */
    ORG_ID,
    CUSTOMER,
    CUSTOMER_NUMBER,
    ORDER_NUMBER,
    LINE_NUMBER,
    ORDER_TYPE,
    DATE_ORDERED,
    ORDERED_ITEM,
    QTY,
    HOLD_NAME,
    HOLD_DESCRIPTION,
    HOLD_AT,
    HOLD_UNTIL,
    HOLD_APPLIED_DATE,
    RELEASED_DATE,
    RELEASED_REASON,
    RELEASE_COMMENT,
    RESPONSIBILITY_NAME,
    HOLD_TYPE,
    HEADER_ORDER_STATUS,
    LINE_ORDER_STATUS,
    WF_ITEM,
    WF_ACTIVITY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || HEADER_ID AS HEADER_ID_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || LINE_TYPE_ID AS LINE_TYPE_ID_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || ORDER_HOLD_ID AS ORDER_HOLD_ID_key,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) /* (select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || RESPONSIBILITY_ID as RESPONSIBILITY_ID_key, */ || '-' || ORG_ID AS ORG_ID_key,
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
    ) || '-' || COALESCE(HEADER_ID, 0) || '-' || COALESCE(ORDER_HOLD_ID, 0) /* || '-' || nvl(responsibility_id, 0) */ AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      OOHL.HEADER_ID,
      OOLA.LINE_TYPE_ID,
      OHL.ORDER_HOLD_ID,
      OHL.CREATED_BY AS HOLD_CREATED_BY,
      OHR.CREATED_BY AS RELEASE_CREATED_BY, /* ,OHA.RESPONSIBILITY_ID */
      OHL.ORG_ID,
      BCDV.PARTY_NAME AS CUSTOMER,
      BCDV.ACCOUNT_NUMBER AS CUSTOMER_NUMBER,
      OOHL.ORDER_NUMBER AS ORDER_NUMBER,
      CASE
        WHEN OHL.LINE_ID IS NULL
        THEN NULL
        ELSE OOLA.LINE_NUMBER || '.' || OOLA.SHIPMENT_NUMBER
      END AS LINE_NUMBER,
      OTTT_H.NAME AS ORDER_TYPE,
      OOHL.ORDERED_DATE AS DATE_ORDERED,
      OOLA.ORDERED_ITEM,
      OOLA.ORDERED_QUANTITY AS QTY,
      OHD.NAME AS HOLD_NAME,
      OHD.DESCRIPTION AS HOLD_DESCRIPTION,
      CASE WHEN OHL.LINE_ID IS NULL THEN 'Order' ELSE 'Line' END AS HOLD_AT,
      OHS.HOLD_UNTIL_DATE AS HOLD_UNTIL,
      OHL.CREATION_DATE AS HOLD_APPLIED_DATE,
      OHR.CREATION_DATE AS RELEASED_DATE,
      OHR.RELEASE_REASON_CODE AS RELEASED_REASON,
      OHR.RELEASE_COMMENT, /* ,FRTL.RESPONSIBILITY_NAME  */
      HT.MEANING AS HOLD_TYPE,
      OOHL.FLOW_STATUS_CODE AS HEADER_ORDER_STATUS,
      OOLA.FLOW_STATUS_CODE AS LINE_ORDER_STATUS,
      OHD.ITEM_TYPE AS WF_ITEM,
      OHD.ACTIVITY_NAME AS WF_ACTIVITY,
      GROUP_CONCAT(FRTL.RESPONSIBILITY_NAME, '; ') WITHIN GROUP (ORDER BY
        OOHL.HEADER_ID NULLS LAST) AS RESPONSIBILITY_NAME
    FROM (
      SELECT
        *
      FROM silver_bec_ods.OE_ORDER_HOLDS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS OHL
    INNER JOIN (
      SELECT
        *
      FROM silver_bec_ods.OE_ORDER_HEADERS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS OOHL
      ON OHL.HEADER_ID = OOHL.HEADER_ID
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.OE_ORDER_LINES_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS OOLA
      ON OHL.LINE_ID = OOLA.LINE_ID
      AND OOHL.HEADER_ID = OOLA.HEADER_ID
      AND OOHL.ORG_ID = OOLA.ORG_ID
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.OE_TRANSACTION_TYPES_TL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS OTTT_H
      ON OOHL.ORDER_TYPE_ID = OTTT_H.TRANSACTION_TYPE_ID AND OTTT_H.LANGUAGE = 'US'
    LEFT OUTER JOIN gold_bec_dwh.DIM_CUSTOMER_DETAILS AS BCDV
      ON OOHL.SHIP_TO_ORG_ID = BCDV.SITE_USE_ID AND OOHL.SOLD_TO_ORG_ID = BCDV.CUST_ACCOUNT_ID
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.OE_HOLD_SOURCES_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS OHS
      ON OHL.HOLD_SOURCE_ID = OHS.HOLD_SOURCE_ID
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.OE_HOLD_RELEASES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS OHR
      ON OHL.HOLD_RELEASE_ID = OHR.HOLD_RELEASE_ID
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.OE_HOLD_DEFINITIONS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS OHD
      ON OHS.HOLD_ID = OHD.HOLD_ID
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.OE_HOLD_AUTHORIZATIONS
      WHERE
        is_deleted_flg <> 'Y'
    ) AS OHA
      ON OHD.HOLD_ID = OHA.HOLD_ID AND OHA.AUTHORIZED_ACTION_CODE = 'REMOVE'
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.FND_RESPONSIBILITY_TL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS FRTL
      ON OHA.RESPONSIBILITY_ID = FRTL.RESPONSIBILITY_ID AND FRTL.LANGUAGE = 'US'
    LEFT OUTER JOIN (
      SELECT
        *
      FROM silver_bec_ods.FND_LOOKUP_VALUES
      WHERE
        is_deleted_flg <> 'Y'
    ) AS HT
      ON OHD.TYPE_CODE = HT.LOOKUP_CODE AND HT.LOOKUP_TYPE = 'HOLD_TYPE' AND HT.language = 'US'
    WHERE
      1 = 1
      AND (
        OOHL.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_om_holds' AND batch_name = 'om'
        )
        OR OHL.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_om_holds' AND batch_name = 'om'
        )
      )
    GROUP BY
      OOHL.HEADER_ID,
      OOLA.LINE_TYPE_ID,
      OHL.ORDER_HOLD_ID,
      OHL.CREATED_BY,
      OHR.CREATED_BY, /* ,OHA.RESPONSIBILITY_ID */
      OHL.ORG_ID,
      BCDV.PARTY_NAME,
      BCDV.ACCOUNT_NUMBER,
      OOHL.ORDER_NUMBER,
      CASE
        WHEN OHL.LINE_ID IS NULL
        THEN NULL
        ELSE OOLA.LINE_NUMBER || '.' || OOLA.SHIPMENT_NUMBER
      END,
      OTTT_H.NAME,
      OOHL.ORDERED_DATE,
      OOLA.ORDERED_ITEM,
      OOLA.ORDERED_QUANTITY,
      OHD.NAME,
      OHD.DESCRIPTION,
      CASE WHEN OHL.LINE_ID IS NULL THEN 'Order' ELSE 'Line' END,
      OHS.HOLD_UNTIL_DATE,
      OHL.CREATION_DATE,
      OHR.CREATION_DATE,
      OHR.RELEASE_REASON_CODE,
      OHR.RELEASE_COMMENT, /* ,FRTL.RESPONSIBILITY_NAME  */
      HT.MEANING,
      OOHL.FLOW_STATUS_CODE,
      OOLA.FLOW_STATUS_CODE,
      OHD.ITEM_TYPE,
      OHD.ACTIVITY_NAME
  )
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_om_holds' AND batch_name = 'om';