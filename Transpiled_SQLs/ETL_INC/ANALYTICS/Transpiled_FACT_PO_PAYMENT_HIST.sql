/* Delete Records */
DELETE FROM gold_bec_dwh.FACT_PO_PAYMENT_HIST
WHERE
  (COALESCE(PO_HEADER_ID, 0), COALESCE(PO_LINE_ID, 0), COALESCE(LINE_LOCATION_ID, 0), COALESCE(SHIPMENT_LINE_ID, 0), COALESCE(CODE_COMBINATION_ID, 0), COALESCE(RECEIPT_ENTERED_AS_DATE, '1900-01-01'), COALESCE(receipt_number, 'NA')) IN (
    SELECT
      COALESCE(TMP.PO_HEADER_ID, 0) AS PO_HEADER_ID,
      COALESCE(TMP.PO_LINE_ID, 0) AS PO_LINE_ID,
      COALESCE(TMP.LINE_LOCATION_ID, 0) AS LINE_LOCATION_ID,
      COALESCE(TMP.SHIPMENT_LINE_ID, 0) AS SHIPMENT_LINE_ID,
      COALESCE(TMP.CODE_COMBINATION_ID, 0) AS CODE_COMBINATION_ID,
      COALESCE(TMP.RECEIPT_ENTERED_AS_DATE, '1900-01-01') AS RECEIPT_ENTERED_AS_DATE,
      COALESCE(TMP.receipt_number, 'NA') AS RECEIPT_NUMBER
    FROM gold_bec_dwh.FACT_PO_PAYMENT_HIST AS dw, (
      SELECT
        POF.PO_HEADER_ID,
        POF.PO_LINE_ID,
        POF.LINE_LOCATION_ID,
        POF.RECEIPT_ENTERED_AS_DATE,
        POF.SHIPMENT_LINE_ID,
        POF.CODE_COMBINATION_ID,
        POF.RECEIPT_NUMBER
      FROM (
        SELECT
          POH.PO_HEADER_ID,
          POL.PO_LINE_ID,
          POLL.LINE_LOCATION_ID,
          RT.TRANSACTION_DATE AS RECEIPT_ENTERED_AS_DATE,
          RSL.SHIPMENT_LINE_ID,
          POD.CODE_COMBINATION_ID,
          RSH.RECEIPT_NUM AS RECEIPT_NUMBER
        FROM silver_bec_ods.PO_HEADERS_ALL AS POH, silver_bec_ods.PO_LINES_ALL AS POL, silver_bec_ods.PO_LINE_LOCATIONS_ALL AS POLL, silver_bec_ods.RCV_SHIPMENT_LINES AS RSL, silver_bec_ods.RCV_SHIPMENT_HEADERS AS RSH, silver_bec_ods.RCV_TRANSACTIONS AS RT, silver_bec_ods.PO_LINE_TYPES_tl AS PLT, silver_bec_ods.PO_RELEASES_ALL AS PORL, silver_bec_ods.PO_DISTRIBUTIONS_ALL AS POD
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
          AND (
            COALESCE(POD.kca_seq_date) > (
              SELECT
                (
                  executebegints - prune_days
                )
              FROM bec_etl_ctrl.batch_dw_info
              WHERE
                dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
            )
            OR COALESCE(RSL.kca_seq_date) > (
              SELECT
                (
                  executebegints - prune_days
                )
              FROM bec_etl_ctrl.batch_dw_info
              WHERE
                dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
            )
            OR COALESCE(POLL.kca_seq_date) > (
              SELECT
                (
                  executebegints - prune_days
                )
              FROM bec_etl_ctrl.batch_dw_info
              WHERE
                dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
            )
            OR COALESCE(POL.kca_seq_date) > (
              SELECT
                (
                  executebegints - prune_days
                )
              FROM bec_etl_ctrl.batch_dw_info
              WHERE
                dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
            )
            OR COALESCE(POH.kca_seq_date) > (
              SELECT
                (
                  executebegints - prune_days
                )
              FROM bec_etl_ctrl.batch_dw_info
              WHERE
                dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
            )
            OR POH.is_deleted_flg = 'Y'
            OR POL.is_deleted_flg = 'Y'
            OR POLL.is_deleted_flg = 'Y'
            OR RSL.is_deleted_flg = 'Y'
            OR RSH.is_deleted_flg = 'Y'
            OR RT.is_deleted_flg = 'Y'
            OR PLT.is_deleted_flg = 'Y'
            OR PORL.is_deleted_flg = 'Y'
            OR POD.is_deleted_flg = 'Y'
          )
        UNION
        SELECT
          POH.PO_HEADER_ID,
          POL.PO_LINE_ID,
          POLL.LINE_LOCATION_ID,
          RT.TRANSACTION_DATE AS RECEIPT_ENTERED_AS_DATE,
          RSL.SHIPMENT_LINE_ID,
          POD.CODE_COMBINATION_ID,
          RSH.RECEIPT_NUM AS RECEIPT_NUMBER
        FROM silver_bec_ods.PO_HEADERS_ALL AS POH, silver_bec_ods.PO_LINES_ALL AS POL, silver_bec_ods.PO_LINE_LOCATIONS_ALL AS POLL, silver_bec_ods.RCV_SHIPMENT_LINES AS RSL, silver_bec_ods.RCV_TRANSACTIONS AS RT, silver_bec_ods.RCV_SHIPMENT_HEADERS AS RSH, silver_bec_ods.PO_LINE_TYPES_tl AS PLT, silver_bec_ods.PO_DISTRIBUTIONS_ALL AS POD
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
          AND (
            COALESCE(POD.kca_seq_date) > (
              SELECT
                (
                  executebegints - prune_days
                )
              FROM bec_etl_ctrl.batch_dw_info
              WHERE
                dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
            )
            OR COALESCE(RSL.kca_seq_date) > (
              SELECT
                (
                  executebegints - prune_days
                )
              FROM bec_etl_ctrl.batch_dw_info
              WHERE
                dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
            )
            OR COALESCE(POLL.kca_seq_date) > (
              SELECT
                (
                  executebegints - prune_days
                )
              FROM bec_etl_ctrl.batch_dw_info
              WHERE
                dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
            )
            OR COALESCE(POL.kca_seq_date) > (
              SELECT
                (
                  executebegints - prune_days
                )
              FROM bec_etl_ctrl.batch_dw_info
              WHERE
                dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
            )
            OR COALESCE(POH.kca_seq_date) > (
              SELECT
                (
                  executebegints - prune_days
                )
              FROM bec_etl_ctrl.batch_dw_info
              WHERE
                dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
            )
            OR POH.is_deleted_flg = 'Y'
            OR POL.is_deleted_flg = 'Y'
            OR POLL.is_deleted_flg = 'Y'
            OR RSL.is_deleted_flg = 'Y'
            OR RSH.is_deleted_flg = 'Y'
            OR RT.is_deleted_flg = 'Y'
            OR PLT.is_deleted_flg = 'Y'
            OR POD.is_deleted_flg = 'Y'
          )
      ) AS POF
    ) AS TMP
    WHERE
      1 = 1
      AND dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(TMP.PO_HEADER_ID, 0) || '-' || COALESCE(TMP.PO_LINE_ID, 0) || '-' || COALESCE(TMP.LINE_LOCATION_ID, 0) || '-' || COALESCE(TMP.SHIPMENT_LINE_ID, 0) || '-' || COALESCE(TMP.CODE_COMBINATION_ID, 0) || '-' || COALESCE(TMP.RECEIPT_ENTERED_AS_DATE, '1900-01-01') || '-' || COALESCE(TMP.receipt_number, 'NA')
  );
/* Insert Records */
INSERT INTO gold_bec_dwh.FACT_PO_PAYMENT_HIST (
  org_id,
  po_header_id,
  po_line_id,
  item_id,
  po_release_id,
  line_location_id,
  po_type,
  po_number,
  agent_id,
  po_release_number,
  po_date,
  po_line_num,
  line_quantity,
  po_unit_of_measure,
  need_by_date,
  promised_date,
  rcv_quantity_received,
  po_unit_price,
  extended_po_rcv_price,
  receipt_number,
  receipt_date,
  receipt_entered_as_date,
  vendor_id,
  vendor_site_id,
  closed_code,
  authorization_status,
  po_currency_code,
  shipment_header_id,
  shipment_line_id,
  line_type,
  po_header_cancel,
  po_line_cancel,
  po_shipment_cancel,
  code_combination_id,
  cvmi_flag,
  org_id_key,
  item_id_key,
  agent_id_key,
  vendor_id_key,
  vendor_site_id_key,
  gl_account_id_key,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
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
      AND (
        COALESCE(POD.kca_seq_date) > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
        )
        OR COALESCE(RSL.kca_seq_date) > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
        )
        OR COALESCE(POLL.kca_seq_date) > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
        )
        OR COALESCE(POL.kca_seq_date) > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
        )
        OR COALESCE(POH.kca_seq_date) > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
        )
      )
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
      AND (
        COALESCE(POD.kca_seq_date) > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
        )
        OR COALESCE(RSL.kca_seq_date) > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
        )
        OR COALESCE(POLL.kca_seq_date) > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
        )
        OR COALESCE(POL.kca_seq_date) > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
        )
        OR COALESCE(POH.kca_seq_date) > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po'
        )
      )
  ) AS POF
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_po_payment_hist' AND batch_name = 'po';