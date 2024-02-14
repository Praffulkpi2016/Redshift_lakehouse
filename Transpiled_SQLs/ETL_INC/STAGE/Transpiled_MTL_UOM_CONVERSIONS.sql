TRUNCATE table bronze_bec_ods_stg.MTL_UOM_CONVERSIONS;
INSERT INTO bronze_bec_ods_stg.MTL_UOM_CONVERSIONS (
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
    kca_operation,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_UOM_CONVERSIONS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (INVENTORY_ITEM_ID, UNIT_OF_MEASURE, kca_seq_id) IN (
      SELECT
        INVENTORY_ITEM_ID,
        UNIT_OF_MEASURE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MTL_UOM_CONVERSIONS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        INVENTORY_ITEM_ID,
        UNIT_OF_MEASURE
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_uom_conversions'
      )
    )
);