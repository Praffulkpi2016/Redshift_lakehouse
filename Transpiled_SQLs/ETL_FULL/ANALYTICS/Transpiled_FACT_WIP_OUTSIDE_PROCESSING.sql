DROP table IF EXISTS gold_bec_dwh.FACT_WIP_OUTSIDE_PROCESSING;
CREATE TABLE gold_bec_dwh.FACT_WIP_OUTSIDE_PROCESSING AS
SELECT
  WE.WIP_ENTITY_ID,
  DJ.PRIMARY_ITEM_ID,
  DJ.ORGANIZATION_ID,
  SI2.INVENTORY_ITEM_ID,
  POH.PO_HEADER_ID,
  POL.PO_LINE_ID,
  POLL.LINE_LOCATION_ID,
  POD.PO_DISTRIBUTION_ID,
  POR.PO_RELEASE_ID,
  BR.RESOURCE_ID,
  WE.WIP_ENTITY_NAME AS DISCRETE_JOB,
  DJ.DESCRIPTION AS JOB_DESCRIPTION,
  SI1.SEGMENT1 AS ASSEMBLY,
  SI1.DESCRIPTION AS ASSY_DESCRIPTION,
  SI1.PRIMARY_UOM_CODE AS UOM,
  DJ.START_QUANTITY AS SCHED_QUANTITY,
  DJ.QUANTITY_COMPLETED AS QUANTITY_COMPLETED,
  DATE_FORMAT(DJ.SCHEDULED_START_DATE, 'dd-MON-yy') || DATE_FORMAT(DJ.SCHEDULED_START_DATE, ' HH:mm') AS SCHED_START_DATE,
  DATE_FORMAT(DJ.SCHEDULED_COMPLETION_DATE, 'dd-MON-yy') || DATE_FORMAT(DJ.SCHEDULED_COMPLETION_DATE, ' HH:mm') AS SCHED_COMPLETION_DATE,
  0 AS RATE,
  LU1.MEANING AS STATUS,
  LU2.MEANING AS JOB_TYPE,
  DJ.STATUS_TYPE,
  POD.WIP_OPERATION_SEQ_NUM AS OP_SEQ,
  POD.WIP_RESOURCE_SEQ_NUM AS SEQ_NUM,
  BR.RESOURCE_CODE AS RESOURCE_CODE,
  POH.SEGMENT1 AS PO_NUMBER,
  POR.RELEASE_NUM AS RELEASE_NUM,
  MEV.FULL_NAME AS BUYER,
  APS.VENDOR_NAME,
  POL.LINE_NUM AS PO_LINE,
  UOM.UOM_CODE AS UNIT_OF_MEAS,
  POLL.SHIPMENT_NUM AS PO_SHIPMENT,
  COALESCE(POLL.PROMISED_DATE, POLL.NEED_BY_DATE) AS DUE_DATE,
  POD.QUANTITY_ORDERED AS QTY_ORDERED,
  POD.QUANTITY_DELIVERED AS QTY_DELIVERED,
  'N' AS IS_DELETED_FLG,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || WE.WIP_ENTITY_ID AS WIP_ENTITY_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || SI2.INVENTORY_ITEM_ID AS INVENTORY_ITEM_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || DJ.ORGANIZATION_ID AS ORGANIZATION_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || DJ.PRIMARY_ITEM_ID AS PRIMARY_ITEM_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || POH.PO_HEADER_ID AS PO_HEADER_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || POL.PO_LINE_ID AS PO_LINE_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || POLL.LINE_LOCATION_ID AS LINE_LOCATION_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || POD.PO_DISTRIBUTION_ID AS PO_DISTRIBUTION_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || POR.PO_RELEASE_ID AS PO_RELEASE_ID_KEY,
  (
    SELECT
      system_id
    FROM bec_etl_ctrl.etlsourceappid
    WHERE
      source_system = 'EBS'
  ) || '-' || BR.RESOURCE_ID AS RESOURCE_ID_KEY,
  'N' AS is_deleted_flag,
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
  ) || '-' || COALESCE(POD.PO_DISTRIBUTION_ID, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    *
  FROM silver_bec_ods.WIP_DISCRETE_JOBS
  WHERE
    is_deleted_flg <> 'Y'
) AS DJ, (
  SELECT
    *
  FROM silver_bec_ods.WIP_ENTITIES
  WHERE
    is_deleted_flg <> 'Y'
) AS WE, (
  SELECT
    *
  FROM silver_bec_ods.MTL_SYSTEM_ITEMS_B
  WHERE
    is_deleted_flg <> 'Y'
) AS SI2, (
  SELECT
    *
  FROM silver_bec_ods.FND_LOOKUP_VALUES
  WHERE
    is_deleted_flg <> 'Y'
) AS LU1, (
  SELECT
    *
  FROM silver_bec_ods.FND_LOOKUP_VALUES
  WHERE
    is_deleted_flg <> 'Y'
) AS LU2, (
  SELECT
    *
  FROM silver_bec_ods.PO_HEADERS_ALL
  WHERE
    is_deleted_flg <> 'Y'
) AS POH, (
  SELECT
    *
  FROM silver_bec_ods.MTL_UNITS_OF_MEASURE_TL
  WHERE
    is_deleted_flg <> 'Y'
) AS UOM, (
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
  FROM silver_bec_ods.PO_DISTRIBUTIONS_ALL
  WHERE
    is_deleted_flg <> 'Y'
) AS POD, (
  SELECT
    *
  FROM silver_bec_ods.AP_SUPPLIERS
  WHERE
    is_deleted_flg <> 'Y'
) AS APS, (
  SELECT
    *
  FROM silver_bec_ods.PO_RELEASES_ALL
  WHERE
    is_deleted_flg <> 'Y'
) AS POR, (
  SELECT
    ORG.ORGANIZATION_ID,
    P.PERSON_ID AS EMPLOYEE_ID,
    P.FULL_NAME
  FROM (
    SELECT
      *
    FROM silver_bec_ods.PER_ALL_PEOPLE_F
    WHERE
      is_deleted_flg <> 'Y'
  ) AS P, (
    SELECT
      *
    FROM silver_bec_ods.HR_ALL_ORGANIZATION_UNITS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS ORG
  WHERE
    P.BUSINESS_GROUP_ID + 0 = ORG.BUSINESS_GROUP_ID
    AND FLOOR(CURRENT_TIMESTAMP()) BETWEEN P.EFFECTIVE_START_DATE AND P.EFFECTIVE_END_DATE
    AND NOT P.EMPLOYEE_NUMBER IS NULL
) AS MEV, (
  SELECT
    *
  FROM silver_bec_ods.BOM_RESOURCES
  WHERE
    is_deleted_flg <> 'Y'
) AS BR, (
  SELECT
    *
  FROM silver_bec_ods.MTL_SYSTEM_ITEMS_B
  WHERE
    is_deleted_flg <> 'Y'
) AS SI1, (
  SELECT
    *
  FROM silver_bec_ods.HR_ORGANIZATION_INFORMATION
  WHERE
    is_deleted_flg <> 'Y'
) AS OOG
WHERE
  1 = 1
  AND DJ.ORGANIZATION_ID = OOG.ORGANIZATION_ID
  AND DJ.WIP_ENTITY_ID = WE.WIP_ENTITY_ID
  AND DJ.ORGANIZATION_ID = WE.ORGANIZATION_ID
  AND WE.WIP_ENTITY_ID = POD.WIP_ENTITY_ID
  AND WE.ENTITY_TYPE <> 2
  AND DJ.ORGANIZATION_ID = SI1.ORGANIZATION_ID()
  AND DJ.PRIMARY_ITEM_ID = SI1.INVENTORY_ITEM_ID()
  AND LU1.LOOKUP_TYPE = 'WIP_JOB_STATUS'
  AND LU1.LOOKUP_CODE = DJ.STATUS_TYPE
  AND LU2.LOOKUP_TYPE = 'WIP_DISCRETE_JOB'
  AND LU2.LOOKUP_CODE = DJ.JOB_TYPE
  AND DJ.ORGANIZATION_ID = POD.DESTINATION_ORGANIZATION_ID
  AND COALESCE(CAST(POD.ORG_ID AS STRING), COALESCE(OOG.ORG_INFORMATION3, '-1')) = COALESCE(OOG.ORG_INFORMATION3, '-1')
  AND UPPER(OOG.ORG_INFORMATION_CONTEXT) = 'ACCOUNTING INFORMATION'
  AND POD.DESTINATION_TYPE_CODE = 'SHOP FLOOR'
  AND NOT POD.WIP_ENTITY_ID IS NULL
  AND POH.PO_HEADER_ID = POD.PO_HEADER_ID
  AND POLL.LINE_LOCATION_ID = POD.LINE_LOCATION_ID
  AND POH.PO_HEADER_ID = POL.PO_HEADER_ID
  AND POL.PO_LINE_ID = POLL.PO_LINE_ID
  AND POH.PO_HEADER_ID = POLL.PO_HEADER_ID
  AND CASE
    WHEN COALESCE(POD.PO_RELEASE_ID, -1) = -1
    OR (
      COALESCE(POD.PO_RELEASE_ID, -1) IS NULL AND -1 IS NULL
    )
    THEN POH.AGENT_ID
    ELSE POR.AGENT_ID
  END = MEV.EMPLOYEE_ID
  AND MEV.ORGANIZATION_ID = POD.DESTINATION_ORGANIZATION_ID
  AND POR.PO_RELEASE_ID() = POD.PO_RELEASE_ID
  AND POR.PO_HEADER_ID() = POD.PO_HEADER_ID
  AND POH.VENDOR_ID = APS.VENDOR_ID()
  AND BR.RESOURCE_ID() = POD.BOM_RESOURCE_ID
  AND SI2.INVENTORY_ITEM_ID = POL.ITEM_ID
  AND SI2.ORGANIZATION_ID = POD.DESTINATION_ORGANIZATION_ID
  AND UOM.UNIT_OF_MEASURE = POL.UNIT_MEAS_LOOKUP_CODE;
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_wip_outside_processing' AND batch_name = 'wip';