/* DELETE */
DELETE FROM gold_bec_dwh.DIM_ITEM_COST
WHERE
  EXISTS(
    SELECT
      1
    FROM silver_bec_ods.CST_ITEM_COSTS AS ods
    WHERE
      ods.INVENTORY_ITEM_ID = DIM_ITEM_COST.INVENTORY_ITEM_ID
      AND ods.ORGANIZATION_ID = DIM_ITEM_COST.ORGANIZATION_ID
      AND ods.COST_TYPE_ID = DIM_ITEM_COST.COST_TYPE_ID
      AND ods.kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_dw_info
        WHERE
          dw_table_name = 'dim_item_cost' AND batch_name = 'costing'
      )
  );
/* INSERT */
INSERT INTO gold_bec_dwh.DIM_ITEM_COST
SELECT
  INVENTORY_ITEM_ID,
  ORGANIZATION_ID,
  COST_TYPE_ID,
  OVERHEAD_COST,
  OUTSIDE_PROCESSING_COST,
  MATERIAL_OVERHEAD_COST,
  MATERIAL_COST,
  LOT_SIZE,
  UNBURDENED_COST,
  TL_RESOURCE,
  TL_OVERHEAD,
  TL_OUTSIDE_PROCESSING,
  TL_MATERIAL_OVERHEAD,
  TL_MATERIAL,
  TL_ITEM_COST,
  SHRINKAGE_RATE,
  ROLLUP_ID,
  RESOURCE_COST,
  REQUEST_ID,
  PROGRAM_UPDATE_DATE,
  PROGRAM_ID,
  PROGRAM_APPLICATION_ID,
  PL_RESOURCE,
  PL_OVERHEAD,
  PL_OUTSIDE_PROCESSING,
  PL_MATERIAL_OVERHEAD,
  PL_MATERIAL,
  PL_ITEM_COST,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  LAST_UPDATE_DATE,
  ITEM_COST,
  INVENTORY_ASSET_FLAG,
  DEFAULTED_FLAG,
  CREATION_DATE,
  CREATED_BY,
  COST_UPDATE_ID,
  BURDEN_COST,
  BASED_ON_ROLLUP_FLAG,
  ASSIGNMENT_SET_ID, /* audit columns */
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
  ) || '-' || COALESCE(INVENTORY_ITEM_ID, 0) || '-' || COALESCE(ORGANIZATION_ID, 0) || '-' || COALESCE(COST_TYPE_ID, 0) AS dw_load_id,
  CURRENT_TIMESTAMP() AS dw_insert_date,
  CURRENT_TIMESTAMP() AS dw_update_date
FROM silver_bec_ods.CST_ITEM_COSTS
WHERE
  kca_seq_date > (
    SELECT
      (
        executebegints - prune_days
      )
    FROM bec_etl_ctrl.batch_dw_info
    WHERE
      dw_table_name = 'dim_item_cost' AND batch_name = 'costing'
  );
/* soft delete */
UPDATE gold_bec_dwh.DIM_ITEM_COST SET is_deleted_flg = 'Y'
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM silver_bec_ods.CST_ITEM_COSTS AS ods
    WHERE
      ods.INVENTORY_ITEM_ID = DIM_ITEM_COST.INVENTORY_ITEM_ID
      AND ods.ORGANIZATION_ID = DIM_ITEM_COST.ORGANIZATION_ID
      AND ods.COST_TYPE_ID = DIM_ITEM_COST.COST_TYPE_ID
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_item_cost' AND batch_name = 'costing';