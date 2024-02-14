/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_SUB_INVENTORIES
WHERE
  (COALESCE(ORGANIZATION_ID, 0), COALESCE(SECONDARY_INVENTORY_NAME, 'NA')) IN (
    SELECT
      COALESCE(ods.ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
      COALESCE(ods.SECONDARY_INVENTORY_NAME, 'NA') AS SECONDARY_INVENTORY_NAME
    FROM gold_bec_dwh.DIM_SUB_INVENTORIES AS dw, silver_bec_ods.MTL_SECONDARY_INVENTORIES AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.ORGANIZATION_ID, 0) || '-' || COALESCE(ods.SECONDARY_INVENTORY_NAME, 'NA')
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_sub_inventories' AND batch_name = 'inv'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_SUB_INVENTORIES (
  SECONDARY_INVENTORY_NAME,
  ORGANIZATION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  DISABLE_DATE,
  INVENTORY_ATP_CODE,
  AVAILABILITY_TYPE,
  RESERVABLE_TYPE,
  LOCATOR_TYPE,
  PICKING_ORDER,
  MATERIAL_ACCOUNT,
  MATERIAL_OVERHEAD_ACCOUNT,
  RESOURCE_ACCOUNT,
  OVERHEAD_ACCOUNT,
  OUTSIDE_PROCESSING_ACCOUNT,
  QUANTITY_TRACKED,
  ASSET_INVENTORY,
  SOURCE_TYPE,
  SOURCE_SUBINVENTORY,
  SOURCE_ORGANIZATION_ID,
  REQUISITION_APPROVAL_TYPE,
  EXPENSE_ACCOUNT,
  ENCUMBRANCE_ACCOUNT,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  PREPROCESSING_LEAD_TIME,
  PROCESSING_LEAD_TIME,
  POSTPROCESSING_LEAD_TIME,
  DEMAND_CLASS,
  PROJECT_ID,
  TASK_ID,
  SUBINVENTORY_USAGE,
  NOTIFY_LIST_ID,
  PICK_UOM_CODE,
  DEPRECIABLE_FLAG,
  LOCATION_ID,
  DEFAULT_COST_GROUP_ID,
  STATUS_ID,
  DEFAULT_LOC_STATUS_ID,
  LPN_CONTROLLED_FLAG,
  PICK_METHODOLOGY,
  CARTONIZATION_FLAG,
  DROPPING_ORDER,
  SUBINVENTORY_TYPE,
  PLANNING_LEVEL,
  DEFAULT_COUNT_TYPE_CODE,
  ENABLE_BULK_PICK,
  ENABLE_LOCATOR_ALIAS,
  ENFORCE_ALIAS_UNIQUENESS,
  ENABLE_OPP_CYC_COUNT,
  OPP_CYC_COUNT_DAYS,
  OPP_CYC_COUNT_HEADER_ID,
  OPP_CYC_COUNT_QUANTITY,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    SECONDARY_INVENTORY_NAME,
    ORGANIZATION_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    DESCRIPTION,
    DISABLE_DATE,
    INVENTORY_ATP_CODE,
    AVAILABILITY_TYPE,
    RESERVABLE_TYPE,
    LOCATOR_TYPE,
    PICKING_ORDER,
    MATERIAL_ACCOUNT,
    MATERIAL_OVERHEAD_ACCOUNT,
    RESOURCE_ACCOUNT,
    OVERHEAD_ACCOUNT,
    OUTSIDE_PROCESSING_ACCOUNT,
    QUANTITY_TRACKED,
    ASSET_INVENTORY,
    SOURCE_TYPE,
    SOURCE_SUBINVENTORY,
    SOURCE_ORGANIZATION_ID,
    REQUISITION_APPROVAL_TYPE,
    EXPENSE_ACCOUNT,
    ENCUMBRANCE_ACCOUNT,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    PREPROCESSING_LEAD_TIME,
    PROCESSING_LEAD_TIME,
    POSTPROCESSING_LEAD_TIME,
    DEMAND_CLASS,
    PROJECT_ID,
    TASK_ID,
    SUBINVENTORY_USAGE,
    NOTIFY_LIST_ID,
    PICK_UOM_CODE,
    DEPRECIABLE_FLAG,
    LOCATION_ID,
    DEFAULT_COST_GROUP_ID,
    STATUS_ID,
    DEFAULT_LOC_STATUS_ID,
    LPN_CONTROLLED_FLAG,
    PICK_METHODOLOGY,
    CARTONIZATION_FLAG,
    DROPPING_ORDER,
    SUBINVENTORY_TYPE,
    PLANNING_LEVEL,
    DEFAULT_COUNT_TYPE_CODE,
    ENABLE_BULK_PICK,
    ENABLE_LOCATOR_ALIAS,
    ENFORCE_ALIAS_UNIQUENESS,
    ENABLE_OPP_CYC_COUNT,
    OPP_CYC_COUNT_DAYS,
    OPP_CYC_COUNT_HEADER_ID,
    OPP_CYC_COUNT_QUANTITY, /* audit columns */
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
    ) || '-' || COALESCE(ORGANIZATION_ID, 0) || '-' || COALESCE(SECONDARY_INVENTORY_NAME, 'NA') AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.MTL_SECONDARY_INVENTORIES
  WHERE
    1 = 1
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_sub_inventories' AND batch_name = 'inv'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_SUB_INVENTORIES SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(ORGANIZATION_ID, 0), COALESCE(SECONDARY_INVENTORY_NAME, 'NA')) IN (
    SELECT
      COALESCE(ods.ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
      COALESCE(ods.SECONDARY_INVENTORY_NAME, 'NA') AS SECONDARY_INVENTORY_NAME
    FROM gold_bec_dwh.DIM_SUB_INVENTORIES AS dw, silver_bec_ods.MTL_SECONDARY_INVENTORIES AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.ORGANIZATION_ID, 0) || '-' || COALESCE(ods.SECONDARY_INVENTORY_NAME, 'NA')
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_sub_inventories' AND batch_name = 'inv';