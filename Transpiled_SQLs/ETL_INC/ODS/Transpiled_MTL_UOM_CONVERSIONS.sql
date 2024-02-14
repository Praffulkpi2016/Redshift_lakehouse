/* Delete Records */
DELETE FROM silver_bec_ods.MTL_UOM_CONVERSIONS
WHERE
  (INVENTORY_ITEM_ID, UNIT_OF_MEASURE) IN (
    SELECT
      stg.INVENTORY_ITEM_ID,
      stg.UNIT_OF_MEASURE
    FROM silver_bec_ods.MTL_UOM_CONVERSIONS AS ods, bronze_bec_ods_stg.MTL_UOM_CONVERSIONS AS stg
    WHERE
      ods.INVENTORY_ITEM_ID = stg.INVENTORY_ITEM_ID
      AND ods.UNIT_OF_MEASURE = stg.UNIT_OF_MEASURE
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_UOM_CONVERSIONS (
  unit_of_measure,
  uom_code,
  uom_class,
  inventory_item_id,
  conversion_rate,
  default_conversion_flag,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  disable_date,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  length,
  width,
  height,
  dimension_uom,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    unit_of_measure,
    uom_code,
    uom_class,
    inventory_item_id,
    conversion_rate,
    default_conversion_flag,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    disable_date,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    length,
    width,
    height,
    dimension_uom,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.MTL_UOM_CONVERSIONS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (INVENTORY_ITEM_ID, UNIT_OF_MEASURE, kca_seq_id) IN (
      SELECT
        INVENTORY_ITEM_ID,
        UNIT_OF_MEASURE,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MTL_UOM_CONVERSIONS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        INVENTORY_ITEM_ID,
        UNIT_OF_MEASURE
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_UOM_CONVERSIONS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_UOM_CONVERSIONS SET IS_DELETED_FLG = 'Y'
WHERE
  (INVENTORY_ITEM_ID, UNIT_OF_MEASURE) IN (
    SELECT
      INVENTORY_ITEM_ID,
      UNIT_OF_MEASURE
    FROM bec_raw_dl_ext.MTL_UOM_CONVERSIONS
    WHERE
      (INVENTORY_ITEM_ID, UNIT_OF_MEASURE, KCA_SEQ_ID) IN (
        SELECT
          INVENTORY_ITEM_ID,
          UNIT_OF_MEASURE,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_UOM_CONVERSIONS
        GROUP BY
          INVENTORY_ITEM_ID,
          UNIT_OF_MEASURE
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_uom_conversions';