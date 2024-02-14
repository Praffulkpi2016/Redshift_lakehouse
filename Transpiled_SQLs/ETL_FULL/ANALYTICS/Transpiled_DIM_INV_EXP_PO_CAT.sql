DROP table IF EXISTS gold_bec_dwh.DIM_INV_EXP_PO_CAT;
CREATE TABLE gold_bec_dwh.DIM_INV_EXP_PO_CAT AS
SELECT
  PO_HEADER_ID,
  PO_LINE_ID,
  INVENTORY_ITEM_ID,
  ORGANIZATION_ID,
  CATEGORY_SET_ID,
  STRUCTURE_ID,
  CATEGORY_ID,
  ITEM_NAME,
  ITEM_DESCRIPTION,
  CATEGORY_SET_NAME,
  CATEGORY_SET_DESC,
  ITEM_CATEGORY_SEGMENT1,
  ITEM_CATEGORY_SEGMENT2,
  ITEM_CATEGORY_SEGMENT3,
  ITEM_CATEGORY_SEGMENT4,
  ITEM_CATEGORY_DESC, /* Audit Columns */
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
  ) || '-' || COALESCE(INVENTORY_ITEM_ID, 0) || '-' || COALESCE(ORGANIZATION_ID, 0) || '-' || COALESCE(PO_HEADER_ID, 0) || '-' || COALESCE(PO_LINE_ID, 0) || '-' || COALESCE(CATEGORY_ID, 0) || '-' || COALESCE(CATEGORY_SET_ID, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM (
  SELECT
    PHA.PO_HEADER_ID,
    PLA.PO_LINE_ID,
    NULL AS INVENTORY_ITEM_ID,
    NULL AS ORGANIZATION_ID,
    NULL AS CATEGORY_SET_ID,
    COALESCE(MC.STRUCTURE_ID, 0) AS STRUCTURE_ID,
    COALESCE(PLA.CATEGORY_ID, 0) AS CATEGORY_ID,
    NULL AS `ITEM_NAME`,
    NULL AS ITEM_DESCRIPTION,
    NULL AS CATEGORY_SET_NAME,
    NULL AS CATEGORY_SET_DESC,
    MC.SEGMENT1 AS `ITEM_CATEGORY_SEGMENT1`,
    COALESCE(MC.SEGMENT2, 'LEVEL2') AS `ITEM_CATEGORY_SEGMENT2`,
    COALESCE(MC.SEGMENT3, 'LEVEL3') AS `ITEM_CATEGORY_SEGMENT3`,
    COALESCE(MC.SEGMENT4, 'LEVEL4') AS `ITEM_CATEGORY_SEGMENT4`,
    MC.DESCRIPTION AS `ITEM_CATEGORY_DESC`
  FROM (
    SELECT
      STRUCTURE_ID,
      SEGMENT1,
      SEGMENT2,
      SEGMENT3,
      SEGMENT4,
      DESCRIPTION,
      CATEGORY_ID
    FROM BEC_ODS.MTL_CATEGORIES_B
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS MC, (
    SELECT
      PO_HEADER_ID
    FROM BEC_ODS.PO_HEADERS_ALL
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS PHA, (
    SELECT
      PO_HEADER_ID,
      PO_LINE_ID,
      CATEGORY_ID
    FROM BEC_ODS.PO_LINES_ALL
    WHERE
      IS_DELETED_FLG <> 'Y' AND ITEM_ID IS NULL
  ) AS PLA
  WHERE
    1 = 1 AND PHA.PO_HEADER_ID = PLA.PO_HEADER_ID AND PLA.CATEGORY_ID = MC.CATEGORY_ID
  UNION
  SELECT
    PHA.PO_HEADER_ID,
    PLA.PO_LINE_ID,
    COALESCE(MIC.INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
    COALESCE(MIC.ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
    COALESCE(MIC.CATEGORY_SET_ID, 0) AS CATEGORY_SET_ID,
    COALESCE(MC.STRUCTURE_ID, 0) AS STRUCTURE_ID,
    COALESCE(PLA.CATEGORY_ID, 0) AS CATEGORY_ID,
    MSI.SEGMENT1 AS `ITEM_NAME`,
    PLA.ITEM_DESCRIPTION AS ITEM_DESCRIPTION,
    CSET.CATEGORY_SET_NAME AS CATEGORY_SET_NAME,
    CSET.DESCRIPTION AS CATEGORY_SET_DESC,
    MC.SEGMENT1 AS `ITEM_CATEGORY_SEGMENT1`,
    COALESCE(MC.SEGMENT2, 'LEVEL2') AS `ITEM_CATEGORY_SEGMENT2`,
    COALESCE(MC.SEGMENT3, 'LEVEL3') AS `ITEM_CATEGORY_SEGMENT3`,
    COALESCE(MC.SEGMENT4, 'LEVEL4') AS `ITEM_CATEGORY_SEGMENT4`,
    MC.DESCRIPTION AS `ITEM_CATEGORY_DESC`
  FROM (
    SELECT
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      CATEGORY_SET_ID,
      CATEGORY_ID
    FROM BEC_ODS.MTL_ITEM_CATEGORIES
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS MIC, (
    SELECT
      CATEGORY_SET_NAME,
      CATEGORY_SET_ID,
      DESCRIPTION
    FROM BEC_ODS.MTL_CATEGORY_SETS_TL
    WHERE
      IS_DELETED_FLG <> 'Y' AND LANGUAGE = 'US'
  ) AS CSET, (
    SELECT
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID,
      SEGMENT1
    FROM BEC_ODS.MTL_SYSTEM_ITEMS_B
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS MSI, (
    SELECT
      STRUCTURE_ID,
      SEGMENT1,
      SEGMENT2,
      SEGMENT3,
      SEGMENT4,
      DESCRIPTION,
      CATEGORY_ID
    FROM BEC_ODS.MTL_CATEGORIES_B
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS MC, (
    SELECT
      PO_HEADER_ID
    FROM BEC_ODS.PO_HEADERS_ALL
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS PHA, (
    SELECT
      PO_LINE_ID,
      CATEGORY_ID,
      PO_HEADER_ID,
      ITEM_ID,
      ITEM_DESCRIPTION
    FROM BEC_ODS.PO_LINES_ALL
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS PLA, (
    SELECT
      SHIP_TO_ORGANIZATION_ID,
      PO_LINE_ID,
      PO_HEADER_ID
    FROM BEC_ODS.PO_LINE_LOCATIONS_ALL
    WHERE
      IS_DELETED_FLG <> 'Y'
  ) AS PLLA
  WHERE
    1 = 1
    AND MIC.INVENTORY_ITEM_ID = MSI.INVENTORY_ITEM_ID
    AND MIC.ORGANIZATION_ID = MSI.ORGANIZATION_ID
    AND MIC.CATEGORY_SET_ID = CSET.CATEGORY_SET_ID
    AND MIC.CATEGORY_ID = MC.CATEGORY_ID
    AND PHA.PO_HEADER_ID = PLA.PO_HEADER_ID
    AND PLA.ITEM_ID = MIC.INVENTORY_ITEM_ID
    AND PLLA.SHIP_TO_ORGANIZATION_ID = MSI.ORGANIZATION_ID
    AND PLLA.PO_LINE_ID = PLA.PO_LINE_ID
    AND PLLA.PO_HEADER_ID = PLA.PO_HEADER_ID
    AND MIC.CATEGORY_SET_ID = 1100000043
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_inv_exp_po_cat' AND batch_name = 'inv';