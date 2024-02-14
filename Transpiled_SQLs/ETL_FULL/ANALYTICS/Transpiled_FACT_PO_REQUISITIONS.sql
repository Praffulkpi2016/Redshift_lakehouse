DROP table IF EXISTS gold_bec_dwh.FACT_PO_REQUISITIONS;
CREATE TABLE gold_bec_dwh.FACT_PO_REQUISITIONS AS
(
  SELECT DISTINCT
    RQ.ORG_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RQ.ORG_ID AS ORG_ID_KEY,
    RQ.REQUISITION_HEADER_ID,
    RL.REQUISITION_LINE_ID,
    RD.DISTRIBUTION_ID AS REQ_DISTRIBUTION_ID,
    RL.VENDOR_ID,
    RL.VENDOR_SITE_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RL.VENDOR_ID AS VENDOR_ID_KEY,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RL.VENDOR_SITE_ID AS VENDOR_SITE_ID_KEY,
    RQ.PREPARER_ID,
    RQ.SEGMENT1,
    RL.LINE_NUM,
    RQ.CREATION_DATE AS RH_CREATION_DATE,
    RQ.AUTHORIZATION_STATUS AS REQUISITION_STATUS,
    RQ.CLOSED_CODE AS RH_CLOSED_CODE,
    RL.UNIT_PRICE,
    RL.QUANTITY,
    COALESCE(RL.AMOUNT, RL.UNIT_PRICE * RL.QUANTITY) AS RL_AMOUNT,
    RL.CURRENCY_CODE,
    RL.ITEM_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || RL.ITEM_ID AS ITEM_ID_KEY,
    RL.TO_PERSON_ID AS REQUESTOR,
    RQ.DESCRIPTION AS REQUISITION_DESCRIPTION,
    RL.ITEM_DESCRIPTION AS REQUISITION_LINE_DESCRIPTION,
    RD.CODE_COMBINATION_ID,
    RL.CATEGORY_ID,
    PRL.PO_HEADER_ID,
    PRL.PO_LINE_ID,
    aprvr_new.action_date AS APPROVED_DATE,
    SBQ.EMPLOYEE_ID AS NEXT_APPROVER_ID,
    RD.PROJECT_ID,
    RD.TASK_ID,
    RL.TO_PERSON_ID AS DELIVER_TO,
    RL.NEED_BY_DATE,
    RL.SUGGESTED_BUYER_ID,
    RL.RATE_DATE AS EXCHANGE_DATE,
    RL.RATE AS EXCHANGE_RATE,
    RL.RATE_TYPE AS EXCHANGE_RATE_TYPE,
    RQ.INTERFACE_SOURCE_CODE AS IMPORT_SOURCE,
    RQ.TYPE_LOOKUP_CODE AS REQUISITION_TYPE,
    RQ.CANCEL_FLAG AS REQUISITION_HEADER_CANCEL,
    RL.CANCEL_FLAG AS REQ_LINE_CANCEL,
    RQ.CREATED_BY,
    RQ.START_DATE_ACTIVE,
    RQ.END_DATE_ACTIVE,
    RL.LINE_TYPE_ID,
    RL.LINE_LOCATION_ID,
    RL.SOURCE_TYPE_CODE,
    RL.CLOSED_DATE,
    RL.CLOSED_CODE,
    RL.destination_organization_id AS INVENTORY_ORGANIZATION_ID,
    RL.SUGGESTED_VENDOR_NAME,
    RL.DESTINATION_TYPE_CODE,
    RL.DELIVER_TO_LOCATION_ID,
    RL.QUANTITY_DELIVERED,
    RL.VENDOR_CONTACT_ID,
    RL.CLOSED_REASON,
    RL.QUANTITY_RECEIVED,
    RL.BASE_UNIT_PRICE,
    RD.last_update_date,
    aprvr.employee_id AS next_approver,
    aprvr.employee_id AS last_approver,
    aprvr_new.employee_id AS next_approver_new,
    aprvr_new.employee_id AS last_approver_new,
    PRL.BUYER_ID,
    PRL.CVMI_FLAG,
    PRL.PO_TYPE,
    POR.release_date AS po_date,
    RL.justification,
    RL.urgent_flag,
    RL.unit_meas_lookup_code,
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
    ) || '-' || COALESCE(RD.DISTRIBUTION_ID, 0) || '-' || COALESCE(aprvr.employee_id, 0) || '-' || COALESCE(aprvr_new.employee_id, 0) || '-' || COALESCE(RQ.AUTHORIZATION_STATUS, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.PO_REQUISITION_HEADERS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RQ, (
    SELECT
      *
    FROM silver_bec_ods.PO_REQUISITION_LINES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RL, (
    SELECT
      *
    FROM silver_bec_ods.PO_REQ_DISTRIBUTIONS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS RD, (
    SELECT DISTINCT
      PD.PO_LINE_ID,
      PD.PO_HEADER_ID,
      RD.REQUISITION_LINE_ID,
      POH.AGENT_ID AS BUYER_ID,
      POH.TYPE_LOOKUP_CODE AS PO_TYPE,
      COALESCE(POLL.consigned_flag, 'N') AS CVMI_FLAG,
      MAX(poll.po_release_id) AS po_release_id
    FROM (
      SELECT
        *
      FROM silver_bec_ods.PO_DISTRIBUTIONS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS PD, (
      SELECT
        *
      FROM silver_bec_ods.PO_REQ_DISTRIBUTIONS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS RD, (
      SELECT
        *
      FROM silver_bec_ods.PO_LINE_LOCATIONS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS POLL, (
      SELECT
        *
      FROM silver_bec_ods.PO_HEADERS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS POH
    WHERE
      PD.REQ_DISTRIBUTION_ID() = RD.DISTRIBUTION_ID
      AND PD.PO_HEADER_ID = POLL.PO_HEADER_ID()
      AND PD.PO_LINE_ID = POLL.PO_LINE_ID()
      AND PD.LINE_LOCATION_ID = POLL.LINE_LOCATION_ID()
      AND POLL.PO_HEADER_ID = POH.PO_HEADER_ID()
    GROUP BY
      PD.PO_LINE_ID,
      PD.PO_HEADER_ID,
      RD.REQUISITION_LINE_ID,
      POH.AGENT_ID,
      POH.TYPE_LOOKUP_CODE,
      COALESCE(POLL.consigned_flag, 'N')
  ) AS PRL, (
    SELECT
      EMPLOYEE_ID,
      OBJECT_ID,
      OBJECT_TYPE_CODE,
      SEQUENCE_NUM
    FROM (
      SELECT
        *
      FROM silver_bec_ods.PO_ACTION_HISTORY
      WHERE
        is_deleted_flg <> 'Y'
    ) AS PAH1, (
      SELECT
        *
      FROM silver_bec_ods.PO_REQUISITION_HEADERS_ALL
      WHERE
        is_deleted_flg <> 'Y'
    ) AS RQ
    WHERE
      PAH1.OBJECT_ID = RQ.REQUISITION_HEADER_ID
      AND PAH1.OBJECT_TYPE_CODE = 'REQUISITION'
      AND PAH1.SEQUENCE_NUM = (
        SELECT
          MAX(SEQUENCE_NUM)
        FROM (
          SELECT
            *
          FROM silver_bec_ods.PO_ACTION_HISTORY
          WHERE
            is_deleted_flg <> 'Y'
        ) AS PAH1, (
          SELECT
            *
          FROM silver_bec_ods.PO_REQUISITION_HEADERS_ALL
          WHERE
            is_deleted_flg <> 'Y'
        ) AS RQ
        WHERE
          PAH1.OBJECT_ID = RQ.REQUISITION_HEADER_ID AND PAH1.OBJECT_TYPE_CODE = 'REQUISITION'
      )
  ) AS SBQ, (
    SELECT
      employee_id,
      object_id
    FROM (
      SELECT
        *
      FROM silver_bec_ods.PO_ACTION_HISTORY
      WHERE
        is_deleted_flg <> 'Y'
    )
    WHERE
      (object_id, sequence_num) IN (
        SELECT
          object_id,
          sequence_num
        FROM (
          SELECT
            MAX(SEQUENCE_NUM) AS sequence_num,
            object_id
          FROM (
            SELECT
              *
            FROM silver_bec_ods.PO_ACTION_HISTORY
            WHERE
              is_deleted_flg <> 'Y'
          )
          WHERE
            1 = 1 AND object_type_code = 'REQUISITION'
          GROUP BY
            object_id
        )
      )
  ) AS aprvr, (
    SELECT
      employee_id,
      object_id,
      action_date
    FROM silver_bec_ods.PO_ACTION_HISTORY
    WHERE
      (object_id, sequence_num) IN (
        SELECT
          object_id,
          sequence_num
        FROM (
          SELECT
            object_id,
            CASE
              WHEN MAX(SEQUENCE_NUM) IN (0, 1)
              THEN MAX(SEQUENCE_NUM)
              ELSE MAX(SEQUENCE_NUM) - 1
            END AS sequence_num
          FROM silver_bec_ods.PO_ACTION_HISTORY
          WHERE
            1 = 1 AND is_deleted_flg <> 'Y' AND object_type_code = 'REQUISITION'
          GROUP BY
            object_id
        )
      )
  ) AS aprvr_new, (
    SELECT
      *
    FROM silver_bec_ods.PO_RELEASES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS POR
  WHERE
    1 = 1
    AND RQ.REQUISITION_HEADER_ID = RL.REQUISITION_HEADER_ID
    AND RL.REQUISITION_LINE_ID = RD.REQUISITION_LINE_ID
    AND RL.REQUISITION_LINE_ID = PRL.REQUISITION_LINE_ID()
    AND RQ.REQUISITION_HEADER_ID = SBQ.OBJECT_ID()
    AND RQ.REQUISITION_HEADER_ID = aprvr.OBJECT_ID()
    AND RQ.REQUISITION_HEADER_ID = aprvr_new.OBJECT_ID()
    AND PRL.po_release_id = POR.PO_RELEASE_ID()
    AND PRL.po_header_id = POR.PO_HEADER_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_po_requisitions' AND batch_name = 'po';