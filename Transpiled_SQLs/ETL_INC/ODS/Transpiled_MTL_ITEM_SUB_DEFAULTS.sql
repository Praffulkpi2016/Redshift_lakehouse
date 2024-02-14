/* Delete Records */
DELETE FROM silver_bec_ods.MTL_ITEM_SUB_DEFAULTS
WHERE
  (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(SUBINVENTORY_CODE, 'NA'), COALESCE(DEFAULT_TYPE, 0)) IN (
    SELECT
      COALESCE(stg.INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
      COALESCE(stg.ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
      COALESCE(stg.SUBINVENTORY_CODE, 'NA') AS SUBINVENTORY_CODE,
      COALESCE(stg.DEFAULT_TYPE, 0) AS DEFAULT_TYPE
    FROM silver_bec_ods.MTL_ITEM_SUB_DEFAULTS AS ods, bronze_bec_ods_stg.MTL_ITEM_SUB_DEFAULTS AS stg
    WHERE
      COALESCE(ods.INVENTORY_ITEM_ID, 0) = COALESCE(stg.INVENTORY_ITEM_ID, 0)
      AND COALESCE(ods.ORGANIZATION_ID, 0) = COALESCE(stg.ORGANIZATION_ID, 0)
      AND COALESCE(ods.SUBINVENTORY_CODE, 'NA') = COALESCE(stg.SUBINVENTORY_CODE, 'NA')
      AND COALESCE(ods.DEFAULT_TYPE, 0) = COALESCE(stg.DEFAULT_TYPE, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_ITEM_SUB_DEFAULTS (
  inventory_item_id,
  organization_id,
  subinventory_code,
  default_type,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    inventory_item_id,
    organization_id,
    subinventory_code,
    default_type,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_ITEM_SUB_DEFAULTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(SUBINVENTORY_CODE, 'NA'), COALESCE(DEFAULT_TYPE, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
        COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
        COALESCE(SUBINVENTORY_CODE, 'NA') AS SUBINVENTORY_CODE,
        COALESCE(DEFAULT_TYPE, 0) AS DEFAULT_TYPE,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MTL_ITEM_SUB_DEFAULTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(INVENTORY_ITEM_ID, 0),
        COALESCE(ORGANIZATION_ID, 0),
        COALESCE(SUBINVENTORY_CODE, 'NA'),
        COALESCE(DEFAULT_TYPE, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_ITEM_SUB_DEFAULTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_ITEM_SUB_DEFAULTS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(SUBINVENTORY_CODE, 'NA'), COALESCE(DEFAULT_TYPE, 0)) IN (
    SELECT
      COALESCE(INVENTORY_ITEM_ID, 0),
      COALESCE(ORGANIZATION_ID, 0),
      COALESCE(SUBINVENTORY_CODE, 'NA'),
      COALESCE(DEFAULT_TYPE, 0)
    FROM bec_raw_dl_ext.MTL_ITEM_SUB_DEFAULTS
    WHERE
      (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(ORGANIZATION_ID, 0), COALESCE(SUBINVENTORY_CODE, 'NA'), COALESCE(DEFAULT_TYPE, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(INVENTORY_ITEM_ID, 0),
          COALESCE(ORGANIZATION_ID, 0),
          COALESCE(SUBINVENTORY_CODE, 'NA'),
          COALESCE(DEFAULT_TYPE, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_ITEM_SUB_DEFAULTS
        GROUP BY
          COALESCE(INVENTORY_ITEM_ID, 0),
          COALESCE(ORGANIZATION_ID, 0),
          COALESCE(SUBINVENTORY_CODE, 'NA'),
          COALESCE(DEFAULT_TYPE, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_item_sub_defaults';