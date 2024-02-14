DROP table IF EXISTS gold_bec_dwh.FACT_COST_ELEMENT_DETAILS;
CREATE TABLE gold_bec_dwh.FACT_COST_ELEMENT_DETAILS AS
(
  SELECT
    MSI.INVENTORY_ITEM_ID,
    MSI.ORGANIZATION_ID,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || MSI.ORGANIZATION_ID AS ORGANIZATION_ID_KEY,
    MIC.CATEGORY_SET_ID,
    CIC.COST_TYPE_ID,
    MSI.SEGMENT1 AS ITEM,
    (
      SELECT
        system_id
      FROM bec_etl_ctrl.etlsourceappid
      WHERE
        source_system = 'EBS'
    ) || '-' || MSI.SEGMENT1 AS ITEM_ID_KEY,
    MSI.DESCRIPTION AS ITEM_DESCRIPTION,
    MSI.PRIMARY_UOM_CODE AS UOM,
    CIC.MATERIAL_COST AS MATERIAL_COST,
    CIC.MATERIAL_OVERHEAD_COST AS MATERIAL_OVHD,
    CIC.RESOURCE_COST AS RESOURCE_COST,
    CIC.OUTSIDE_PROCESSING_COST AS OUTSIDE_PROC,
    CIC.OVERHEAD_COST AS OVHD_COST,
    CIC.ITEM_COST AS TOTAL_UNIT_COST,
    CT.COST_TYPE,
    '1' AS EXCHANGE_RATE,
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
    ) || '-' || COALESCE(MSI.INVENTORY_ITEM_ID, 0) || '-' || COALESCE(MSI.ORGANIZATION_ID, 0) || '-' || COALESCE(CIC.COST_TYPE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM (
    SELECT
      *
    FROM silver_bec_ods.MTL_SYSTEM_ITEMS_B
    WHERE
      is_deleted_flg <> 'Y'
  ) AS MSI, (
    SELECT
      *
    FROM silver_bec_ods.MTL_PARAMETERS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS MP, (
    SELECT
      *
    FROM silver_bec_ods.MTL_ITEM_CATEGORIES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS MIC, (
    SELECT
      *
    FROM silver_bec_ods.MTL_CATEGORIES_B
    WHERE
      is_deleted_flg <> 'Y'
  ) AS MC, (
    SELECT
      *
    FROM silver_bec_ods.CST_COST_TYPES
    WHERE
      is_deleted_flg <> 'Y'
  ) AS CT, (
    SELECT
      *
    FROM silver_bec_ods.CST_ITEM_COSTS
    WHERE
      is_deleted_flg <> 'Y'
  ) AS CIC
  WHERE
    1 = 1
    AND CIC.INVENTORY_ITEM_ID = MSI.INVENTORY_ITEM_ID
    AND CIC.ORGANIZATION_ID = MSI.ORGANIZATION_ID
    AND CIC.ORGANIZATION_ID = MP.COST_ORGANIZATION_ID
    AND CIC.INVENTORY_ITEM_ID = MIC.INVENTORY_ITEM_ID
    AND CIC.ORGANIZATION_ID = MIC.ORGANIZATION_ID
    AND MIC.CATEGORY_SET_ID = 1
    AND MIC.CATEGORY_ID = MC.CATEGORY_ID
    AND CIC.COST_TYPE_ID = CT.COST_TYPE_ID
);
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'fact_cost_element_details' AND batch_name = 'costing';