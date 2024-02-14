/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_ITEM_LOCATIONS
WHERE
  (COALESCE(INVENTORY_LOCATION_ID, 0), COALESCE(ORGANIZATION_ID, 0)) IN (
    SELECT
      COALESCE(ods.INVENTORY_LOCATION_ID, 0),
      COALESCE(ods.ORGANIZATION_ID, 0)
    FROM gold_bec_dwh.DIM_ITEM_LOCATIONS AS dw, silver_bec_ods.mtl_item_locations AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.INVENTORY_LOCATION_ID, 0) || '-' || COALESCE(ods.ORGANIZATION_ID, 0)
      AND (
        ods.kca_seq_date > (
          SELECT
            (
              executebegints - prune_days
            )
          FROM bec_etl_ctrl.batch_dw_info
          WHERE
            dw_table_name = 'dim_item_locations' AND batch_name = 'inv'
        )
      )
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_ITEM_LOCATIONS (
  INVENTORY_LOCATION_ID,
  ORGANIZATION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  DESCRIPTIVE_TEXT,
  DISABLE_DATE,
  INVENTORY_LOCATION_TYPE,
  PICKING_ORDER,
  PHYSICAL_LOCATION_CODE,
  LOCATION_MAXIMUM_UNITS,
  SUBINVENTORY_CODE,
  LOCATION_WEIGHT_UOM_CODE,
  MAX_WEIGHT,
  VOLUME_UOM_CODE,
  MAX_CUBIC_AREA,
  X_COORDINATE,
  Y_COORDINATE,
  Z_COORDINATE,
  INVENTORY_ACCOUNT_ID,
  SEGMENT1,
  SEGMENT2,
  SEGMENT3,
  SEGMENT4,
  SEGMENT5,
  SEGMENT6,
  SEGMENT7,
  SEGMENT8,
  SEGMENT9,
  SEGMENT10,
  SUMMARY_FLAG,
  ENABLED_FLAG,
  START_DATE_ACTIVE,
  END_DATE_ACTIVE,
  ATTRIBUTE_CATEGORY,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  PROJECT_ID,
  TASK_ID,
  PHYSICAL_LOCATION_ID,
  PICK_UOM_CODE,
  DIMENSION_UOM_CODE,
  LENGTH,
  WIDTH,
  HEIGHT,
  LOCATOR_STATUS,
  STATUS_ID,
  CURRENT_CUBIC_AREA,
  AVAILABLE_CUBIC_AREA,
  CURRENT_WEIGHT,
  AVAILABLE_WEIGHT,
  LOCATION_CURRENT_UNITS,
  LOCATION_AVAILABLE_UNITS,
  INVENTORY_ITEM_ID,
  SUGGESTED_CUBIC_AREA,
  SUGGESTED_WEIGHT,
  LOCATION_SUGGESTED_UNITS,
  EMPTY_FLAG,
  MIXED_ITEMS_FLAG,
  DROPPING_ORDER,
  AVAILABILITY_TYPE,
  INVENTORY_ATP_CODE,
  RESERVABLE_TYPE,
  ALIAS,
  INV_DOCK_DOOR_ID,
  INV_ORG_ID,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    INVENTORY_LOCATION_ID,
    ORGANIZATION_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    DESCRIPTION,
    DESCRIPTIVE_TEXT,
    DISABLE_DATE,
    INVENTORY_LOCATION_TYPE,
    PICKING_ORDER,
    PHYSICAL_LOCATION_CODE,
    LOCATION_MAXIMUM_UNITS,
    SUBINVENTORY_CODE,
    LOCATION_WEIGHT_UOM_CODE,
    MAX_WEIGHT,
    VOLUME_UOM_CODE,
    MAX_CUBIC_AREA,
    X_COORDINATE,
    Y_COORDINATE,
    Z_COORDINATE,
    INVENTORY_ACCOUNT_ID,
    SEGMENT1,
    SEGMENT2,
    SEGMENT3,
    SEGMENT4,
    SEGMENT5,
    SEGMENT6,
    SEGMENT7,
    SEGMENT8,
    SEGMENT9,
    SEGMENT10,
    SUMMARY_FLAG,
    ENABLED_FLAG,
    START_DATE_ACTIVE,
    END_DATE_ACTIVE,
    ATTRIBUTE_CATEGORY,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    PROJECT_ID,
    TASK_ID,
    PHYSICAL_LOCATION_ID,
    PICK_UOM_CODE,
    DIMENSION_UOM_CODE,
    LENGTH,
    WIDTH,
    HEIGHT,
    LOCATOR_STATUS,
    STATUS_ID,
    CURRENT_CUBIC_AREA,
    AVAILABLE_CUBIC_AREA,
    CURRENT_WEIGHT,
    AVAILABLE_WEIGHT,
    LOCATION_CURRENT_UNITS,
    LOCATION_AVAILABLE_UNITS,
    INVENTORY_ITEM_ID,
    SUGGESTED_CUBIC_AREA,
    SUGGESTED_WEIGHT,
    LOCATION_SUGGESTED_UNITS,
    EMPTY_FLAG,
    MIXED_ITEMS_FLAG,
    DROPPING_ORDER,
    AVAILABILITY_TYPE,
    INVENTORY_ATP_CODE,
    RESERVABLE_TYPE,
    ALIAS,
    INV_DOCK_DOOR_ID,
    INV_ORG_ID,
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
    ) || '-' || COALESCE(INVENTORY_LOCATION_ID, 0) || '-' || COALESCE(ORGANIZATION_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.mtl_item_locations
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
          dw_table_name = 'dim_item_locations' AND batch_name = 'inv'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.DIM_ITEM_LOCATIONS SET is_deleted_flg = 'Y'
WHERE
  NOT (COALESCE(INVENTORY_LOCATION_ID, 0), COALESCE(ORGANIZATION_ID, 0)) IN (
    SELECT
      COALESCE(ods.INVENTORY_LOCATION_ID, 0) AS INVENTORY_LOCATION_ID,
      COALESCE(ods.ORGANIZATION_ID, 0) AS ORGANIZATION_ID
    FROM gold_bec_dwh.DIM_ITEM_LOCATIONS AS dw, silver_bec_ods.mtl_item_locations AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.INVENTORY_LOCATION_ID, 0) || '-' || COALESCE(ods.ORGANIZATION_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_item_locations' AND batch_name = 'inv';