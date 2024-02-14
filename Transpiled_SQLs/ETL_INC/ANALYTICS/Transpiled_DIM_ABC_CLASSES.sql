/* Delete Records */
DELETE FROM gold_bec_dwh.dim_abc_classes
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.MTL_ABC_ASSIGNMENT_GROUPS AS maag
    JOIN silver_bec_ods.MTL_ABC_ASSIGNMENTS AS maa
      ON maag.assignment_group_id = maa.assignment_group_id
    JOIN silver_bec_ods.MTL_ABC_CLASSES AS mac
      ON maa.abc_class_id = mac.abc_class_id
    WHERE
      maag.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_abc_classes' AND batch_name = 'inv'
      )
      AND dim_abc_classes.assignment_group_id = maag.assignment_group_id
      AND dim_abc_classes.abc_class_id = mac.abc_class_id
      AND dim_abc_classes.inventory_item_id = maa.inventory_item_id
      AND dim_abc_classes.organization_id = maag.organization_id
  );
/* Insert records */
INSERT INTO gold_bec_dwh.DIM_ABC_CLASSES (
  ASSIGNMENT_GROUP_ID,
  INVENTORY_ITEM_ID,
  organization_id,
  ABC_CLASS_ID,
  ABC_CLASS_NAME,
  description,
  disable_date,
  ASSIGNMENT_GROUP_NAME,
  COMPILE_ID,
  SECONDARY_INVENTORY,
  ITEM_SCOPE_TYPE,
  CLASSIFICATION_METHOD_TYPE,
  is_deleted_flg,
  SOURCE_APP_ID,
  DW_LOAD_ID,
  DW_INSERT_DATE,
  DW_UPDATE_DATE
)
(
  SELECT
    mAAG.ASSIGNMENT_GROUP_ID,
    MAA.INVENTORY_ITEM_ID,
    maag.organization_id,
    mac.ABC_CLASS_ID,
    MAC.ABC_CLASS_NAME,
    mac.description,
    mac.disable_date,
    maag.ASSIGNMENT_GROUP_NAME,
    COMPILE_ID,
    SECONDARY_INVENTORY,
    ITEM_SCOPE_TYPE,
    CLASSIFICATION_METHOD_TYPE, /* audit columns */
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
    ) || '-' || COALESCE(maa.ASSIGNMENT_GROUP_ID, 0) || '-' || COALESCE(maa.ABC_CLASS_ID, 0) || '-' || COALESCE(maa.INVENTORY_ITEM_ID, 0) || '-' || COALESCE(maag.organization_id, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.MTL_ABC_ASSIGNMENT_GROUPS AS maag, silver_bec_ods.MTL_ABC_ASSIGNMENTS AS maa, silver_bec_ods.MTL_ABC_CLASSES AS mac
  WHERE
    1 = 1
    AND (
      maag.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_abc_classes' AND batch_name = 'inv'
      )
    )
    AND maag.assignment_group_id = MAA.ASSIGNMENT_GROUP_ID
    AND MAA.ABC_CLASS_ID = MAC.ABC_CLASS_ID
);
/* Soft delete */
WITH ValidRecords AS (
  SELECT
    maag.ASSIGNMENT_GROUP_ID,
    maa.ABC_CLASS_ID,
    maa.INVENTORY_ITEM_ID
  FROM silver_bec_ods.MTL_ABC_ASSIGNMENT_GROUPS AS maag
  JOIN silver_bec_ods.MTL_ABC_ASSIGNMENTS AS maa
    ON maag.ASSIGNMENT_GROUP_ID = maa.ASSIGNMENT_GROUP_ID
  JOIN silver_bec_ods.MTL_ABC_CLASSES AS mac
    ON maa.ABC_CLASS_ID = mac.ABC_CLASS_ID
  WHERE
    maag.is_deleted_flg <> 'Y'
)
UPDATE gold_bec_dwh.dim_abc_classes SET is_deleted_flg = 'Y'
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM ValidRecords AS ods
    WHERE
      dim_abc_classes.ASSIGNMENT_GROUP_ID = ods.ASSIGNMENT_GROUP_ID
      AND dim_abc_classes.ABC_CLASS_ID = ods.ABC_CLASS_ID
      AND dim_abc_classes.INVENTORY_ITEM_ID = ods.INVENTORY_ITEM_ID
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_abc_classes' AND batch_name = 'inv';