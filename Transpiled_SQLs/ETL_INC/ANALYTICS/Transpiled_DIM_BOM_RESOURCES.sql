/* Delete Records */
DELETE FROM gold_bec_dwh.DIM_BOM_RESOURCES
WHERE
  (
    COALESCE(RESOURCE_ID, 0)
  ) IN (
    SELECT
      ods.RESOURCE_ID
    FROM gold_bec_dwh.DIM_BOM_RESOURCES AS dw, (
      SELECT
        COALESCE(RESOURCE_ID, 0) AS RESOURCE_ID
      FROM silver_bec_ods.BOM_RESOURCES
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
              dw_table_name = 'dim_bom_resources' AND batch_name = 'wip'
          )
        )
    ) AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.RESOURCE_ID, 0)
  );
/* Insert Records */
INSERT INTO gold_bec_dwh.DIM_BOM_RESOURCES (
  RESOURCE_ID,
  RESOURCE_CODE,
  ORGANIZATION_ID,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_LOGIN,
  DESCRIPTION,
  DISABLE_DATE,
  COST_ELEMENT_ID,
  PURCHASE_ITEM_ID,
  COST_CODE_TYPE,
  FUNCTIONAL_CURRENCY_FLAG,
  UNIT_OF_MEASURE,
  DEFAULT_ACTIVITY_ID,
  RESOURCE_TYPE,
  AUTOCHARGE_TYPE,
  STANDARD_RATE_FLAG,
  DEFAULT_BASIS_TYPE,
  ABSORPTION_ACCOUNT,
  ALLOW_COSTS_FLAG,
  RATE_VARIANCE_ACCOUNT,
  EXPENDITURE_TYPE,
  ATTRIBUTE_CATEGORY,
  ATTRIBUTE1,
  ATTRIBUTE2,
  ATTRIBUTE3,
  ATTRIBUTE4,
  ATTRIBUTE5,
  ATTRIBUTE6,
  ATTRIBUTE7,
  ATTRIBUTE8,
  ATTRIBUTE9,
  ATTRIBUTE10,
  ATTRIBUTE11,
  ATTRIBUTE12,
  ATTRIBUTE13,
  ATTRIBUTE14,
  ATTRIBUTE15,
  REQUEST_ID,
  PROGRAM_APPLICATION_ID,
  PROGRAM_ID,
  PROGRAM_UPDATE_DATE,
  BATCHABLE,
  MAX_BATCH_CAPACITY,
  MIN_BATCH_CAPACITY,
  BATCH_CAPACITY_UOM,
  BATCH_WINDOW,
  BATCH_WINDOW_UOM,
  COMPETENCE_ID,
  RATING_LEVEL_ID,
  QUALIFICATION_TYPE_ID,
  BILLABLE_ITEM_ID,
  SUPPLY_SUBINVENTORY,
  SUPPLY_LOCATOR_ID,
  BATCHING_PENALTY, /* Audit COLUMNS */
  is_deleted_flg,
  source_app_id,
  dw_load_id,
  dw_insert_date,
  dw_update_date
)
(
  SELECT
    RESOURCE_ID,
    RESOURCE_CODE,
    ORGANIZATION_ID,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    LAST_UPDATE_LOGIN,
    DESCRIPTION,
    DISABLE_DATE,
    COST_ELEMENT_ID,
    PURCHASE_ITEM_ID,
    COST_CODE_TYPE,
    FUNCTIONAL_CURRENCY_FLAG,
    UNIT_OF_MEASURE,
    DEFAULT_ACTIVITY_ID,
    RESOURCE_TYPE,
    AUTOCHARGE_TYPE,
    STANDARD_RATE_FLAG,
    DEFAULT_BASIS_TYPE,
    ABSORPTION_ACCOUNT,
    ALLOW_COSTS_FLAG,
    RATE_VARIANCE_ACCOUNT,
    EXPENDITURE_TYPE,
    ATTRIBUTE_CATEGORY,
    ATTRIBUTE1,
    ATTRIBUTE2,
    ATTRIBUTE3,
    ATTRIBUTE4,
    ATTRIBUTE5,
    ATTRIBUTE6,
    ATTRIBUTE7,
    ATTRIBUTE8,
    ATTRIBUTE9,
    ATTRIBUTE10,
    ATTRIBUTE11,
    ATTRIBUTE12,
    ATTRIBUTE13,
    ATTRIBUTE14,
    ATTRIBUTE15,
    REQUEST_ID,
    PROGRAM_APPLICATION_ID,
    PROGRAM_ID,
    PROGRAM_UPDATE_DATE,
    BATCHABLE,
    MAX_BATCH_CAPACITY,
    MIN_BATCH_CAPACITY,
    BATCH_CAPACITY_UOM,
    BATCH_WINDOW,
    BATCH_WINDOW_UOM,
    COMPETENCE_ID,
    RATING_LEVEL_ID,
    QUALIFICATION_TYPE_ID,
    BILLABLE_ITEM_ID,
    SUPPLY_SUBINVENTORY,
    SUPPLY_LOCATOR_ID,
    BATCHING_PENALTY, /* Audit COLUMNS */
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
    ) || '-' || COALESCE(RESOURCE_ID, 0) AS dw_load_id,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
  FROM BEC_ODS.BOM_RESOURCES
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
          dw_table_name = 'dim_bom_resources' AND batch_name = 'wip'
      )
    )
);
/* Soft Delete */
UPDATE gold_bec_dwh.dim_bom_resources SET is_deleted_flg = 'Y'
WHERE
  NOT (
    COALESCE(RESOURCE_ID, 0)
  ) IN (
    SELECT
      COALESCE(ods.RESOURCE_ID, 0) AS RESOURCE_ID
    FROM gold_bec_dwh.dim_bom_resources AS dw, silver_bec_ods.bom_resources AS ods
    WHERE
      dw.dw_load_id = (
        SELECT
          system_id
        FROM bec_etl_ctrl.etlsourceappid
        WHERE
          source_system = 'EBS'
      ) || '-' || COALESCE(ods.RESOURCE_ID, 0)
      AND ods.is_deleted_flg <> 'Y'
  );
UPDATE bec_etl_ctrl.batch_dw_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  dw_table_name = 'dim_bom_resources' AND batch_name = 'wip';