/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_INV_MASTER_ITEMS
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.MTL_SYSTEM_ITEMS_B AS ods
    WHERE
      1 = 1
      AND kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_inv_master_items' AND batch_name = 'ap'
      )
      AND DIM_INV_MASTER_ITEMS.INVENTORY_ITEM_ID = ODS.INVENTORY_ITEM_ID
      AND DIM_INV_MASTER_ITEMS.ORGANIZATION_ID = ODS.ORGANIZATION_ID
  );
/* Insert records */
INSERT INTO gold_bec_dwh.dim_inv_master_items (
  INVENTORY_ITEM_ID,
  ORGANIZATION_ID,
  ENABLED_FLAG,
  ITEM_DESCRIPTION,
  ITEM_NAME,
  ITEM_TYPE,
  PURCHASING_ITEM_FLAG,
  SHIPPABLE_ITEM_FLAG,
  CUSTOMER_ORDER_FLAG,
  SERVICE_ITEM_FLAG,
  EXPENSE_ACCOUNT,
  ENCUMBRANCE_ACCOUNT,
  UNIT_WEIGHT,
  UNIT_VOLUME,
  WEIGHT_UOM_CODE,
  VOLUME_UOM_CODE,
  PRIMARY_UOM_CODE,
  PRIMARY_UNIT_OF_MEASURE,
  CONSIGNED_FLAG,
  program_name,
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    INVENTORY_ITEM_ID,
    ORGANIZATION_ID,
    ENABLED_FLAG,
    DESCRIPTION AS ITEM_DESCRIPTION,
    SEGMENT1 AS ITEM_NAME,
    ITEM_TYPE,
    PURCHASING_ITEM_FLAG,
    SHIPPABLE_ITEM_FLAG,
    CUSTOMER_ORDER_FLAG,
    SERVICE_ITEM_FLAG,
    EXPENSE_ACCOUNT,
    ENCUMBRANCE_ACCOUNT,
    UNIT_WEIGHT,
    UNIT_VOLUME,
    WEIGHT_UOM_CODE,
    VOLUME_UOM_CODE,
    PRIMARY_UOM_CODE,
    PRIMARY_UNIT_OF_MEASURE,
    CONSIGNED_FLAG,
    attribute5 AS program_name,
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
    ) || '-' || COALESCE(INVENTORY_ITEM_ID, 0) || '-' || COALESCE(ORGANIZATION_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM silver_bec_ods.MTL_SYSTEM_ITEMS_B
  /* WHERE ORGANIZATION_ID = 204 */
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
          dw_table_name = 'dim_inv_master_items' AND batch_name = 'ap'
      )
    )
);
/* Soft delete */
UPDATE gold_bec_dwh.dim_inv_master_items SET is_deleted_flg = 'Y'
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM silver_bec_ods.MTL_SYSTEM_ITEMS_B AS ods
    WHERE
      1 = 1
      AND is_deleted_flg <> 'Y'
      AND DIM_INV_MASTER_ITEMS.INVENTORY_ITEM_ID = ODS.INVENTORY_ITEM_ID
      AND DIM_INV_MASTER_ITEMS.ORGANIZATION_ID = ODS.ORGANIZATION_ID
  );
UPDATE bec_etl_ctrl.batch_dw_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_inv_master_items' AND batch_name = 'ap';