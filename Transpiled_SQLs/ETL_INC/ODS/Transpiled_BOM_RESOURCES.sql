/* Delete Records */
DELETE FROM silver_bec_ods.BOM_RESOURCES
WHERE
  (
    COALESCE(RESOURCE_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.RESOURCE_ID, 0) AS RESOURCE_ID
    FROM silver_bec_ods.BOM_RESOURCES AS ods, bronze_bec_ods_stg.BOM_RESOURCES AS stg
    WHERE
      COALESCE(ods.RESOURCE_ID, 0) = COALESCE(stg.RESOURCE_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.BOM_RESOURCES (
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
  BATCHING_PENALTY,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
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
    BATCHING_PENALTY,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.BOM_RESOURCES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(RESOURCE_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(RESOURCE_ID, 0) AS RESOURCE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.BOM_RESOURCES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(RESOURCE_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.BOM_RESOURCES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.BOM_RESOURCES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    RESOURCE_ID
  ) IN (
    SELECT
      RESOURCE_ID
    FROM bec_raw_dl_ext.BOM_RESOURCES
    WHERE
      (RESOURCE_ID, KCA_SEQ_ID) IN (
        SELECT
          RESOURCE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.BOM_RESOURCES
        GROUP BY
          RESOURCE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'bom_resources';