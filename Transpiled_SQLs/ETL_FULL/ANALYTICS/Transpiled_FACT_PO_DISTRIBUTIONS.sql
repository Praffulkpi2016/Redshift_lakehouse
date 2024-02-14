DROP table IF EXISTS gold_bec_dwh.FACT_PO_DISTRIBUTIONS;
CREATE TABLE gold_bec_dwh.FACT_PO_DISTRIBUTIONS AS
(
  SELECT
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || D.PO_DISTRIBUTION_ID AS PO_DISTRIBUTION_ID_KEY,
    D.PO_DISTRIBUTION_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || D.PO_HEADER_ID AS PO_HEADER_ID_KEY,
    D.PO_HEADER_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || D.PO_LINE_ID AS PO_LINE_ID_KEY,
    D.PO_LINE_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || D.LINE_LOCATION_ID AS LINE_LOCATION_ID_KEY,
    D.LINE_LOCATION_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || D.PO_RELEASE_ID AS PO_RELEASE_ID_KEY,
    D.PO_RELEASE_ID,
    D.CREATION_DATE AS `DIST_CREATION_DATE`,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || D.CODE_COMBINATION_ID AS CODE_COMBINATION_ID_KEY,
    D.CODE_COMBINATION_ID,
    D.QUANTITY_BILLED,
    D.QUANTITY_CANCELLED,
    D.QUANTITY_DELIVERED,
    D.QUANTITY_FINANCED,
    D.QUANTITY_ORDERED,
    D.QUANTITY_RECOUPED,
    D.AMOUNT_BILLED,
    D.AMOUNT_CANCELLED,
    D.AMOUNT_DELIVERED,
    D.AMOUNT_FINANCED,
    D.AMOUNT_ORDERED,
    D.AMOUNT_RECOUPED,
    D.AMOUNT_TO_ENCUMBER,
    D.ENCUMBERED_FLAG,
    D.ENCUMBERED_AMOUNT,
    D.DISTRIBUTION_NUM,
    D.DISTRIBUTION_TYPE,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || D.PROJECT_ID AS PROJECT_ID_KEY,
    D.PROJECT_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || D.TASK_ID AS TASK_ID_KEY,
    D.TASK_ID,
    D.AWARD_ID,
    D.EXPENDITURE_ITEM_DATE,
    D.EXPENDITURE_TYPE,
    D.EXPENDITURE_ORGANIZATION_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || D.ORG_ID AS ORG_ID_KEY,
    D.ORG_ID,
    D.LAST_UPDATE_DATE,
    P.LINE_NUM,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || P.CATEGORY_ID AS CATEGORY_ID_KEY,
    P.CATEGORY_ID,
    P.ITEM_ID AS `INVENTORY_ITEM_ID`,
    P.LIST_PRICE_PER_UNIT,
    P.UNIT_PRICE,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || F.INVENTORY_ORGANIZATION_ID AS INVENTORY_ORGANIZATION_ID_KEY,
    F.INVENTORY_ORGANIZATION_ID,
    H.TYPE_LOOKUP_CODE,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || H.AGENT_ID AS AGENT_ID_KEY,
    H.AGENT_ID,
    H.SEGMENT1 AS `PO_NUMBER`,
    H.CURRENCY_CODE,
    H.CREATION_DATE AS `PO_DATE`,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || H.VENDOR_SITE_ID AS VENDOR_SITE_ID_KEY,
    H.VENDOR_SITE_ID,
    F.SET_OF_BOOKS_ID,
    P.item_description AS PO_DESCRIPTION,
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
    ) || '-' || COALESCE(D.PO_DISTRIBUTION_ID, 0) || '-' || COALESCE(D.PO_HEADER_ID, 0) || '-' || COALESCE(D.PO_LINE_ID, 0) || '-' || COALESCE(F.SET_OF_BOOKS_ID, 0) || '-' || COALESCE(D.ORG_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.PO_DISTRIBUTIONS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS D, (
    SELECT
      *
    FROM silver_bec_ods.PO_HEADERS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS H, (
    SELECT
      *
    FROM silver_bec_ods.PO_LINES_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS P, (
    SELECT
      *
    FROM silver_bec_ods.FINANCIALS_SYSTEM_PARAMS_ALL
    WHERE
      is_deleted_flg <> 'Y'
  ) AS F
  WHERE
    D.PO_HEADER_ID = H.PO_HEADER_ID()
    AND D.PO_LINE_ID = P.PO_LINE_ID()
    AND P.ORG_ID = F.ORG_ID()
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_po_distributions' AND batch_name = 'po';