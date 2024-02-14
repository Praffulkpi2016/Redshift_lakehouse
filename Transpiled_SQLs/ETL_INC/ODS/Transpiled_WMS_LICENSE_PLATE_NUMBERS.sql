/* Delete Records */
DELETE FROM silver_bec_ods.WMS_LICENSE_PLATE_NUMBERS
WHERE
  (
    COALESCE(LPN_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.LPN_ID, 0) AS LPN_ID
    FROM silver_bec_ods.WMS_LICENSE_PLATE_NUMBERS AS ods, bronze_bec_ods_stg.WMS_LICENSE_PLATE_NUMBERS AS stg
    WHERE
      COALESCE(ods.LPN_ID, 0) = COALESCE(stg.LPN_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.WMS_LICENSE_PLATE_NUMBERS (
  lpn_id,
  license_plate_number,
  inventory_item_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  revision,
  lot_number,
  serial_number,
  organization_id,
  subinventory_code,
  locator_id,
  parent_lpn_id,
  gross_weight_uom_code,
  gross_weight,
  content_volume_uom_code,
  content_volume,
  tare_weight_uom_code,
  tare_weight,
  status_id,
  lpn_state,
  sealed_status,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  cost_group_id,
  lpn_context,
  lpn_reusability,
  outermost_lpn_id,
  homogeneous_container,
  source_type_id,
  source_header_id,
  source_line_id,
  source_line_detail_id,
  source_name,
  container_volume,
  container_volume_uom,
  catch_weight_flag,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    lpn_id,
    license_plate_number,
    inventory_item_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    revision,
    lot_number,
    serial_number,
    organization_id,
    subinventory_code,
    locator_id,
    parent_lpn_id,
    gross_weight_uom_code,
    gross_weight,
    content_volume_uom_code,
    content_volume,
    tare_weight_uom_code,
    tare_weight,
    status_id,
    lpn_state,
    sealed_status,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    cost_group_id,
    lpn_context,
    lpn_reusability,
    outermost_lpn_id,
    homogeneous_container,
    source_type_id,
    source_header_id,
    source_line_id,
    source_line_detail_id,
    source_name,
    container_volume,
    container_volume_uom,
    catch_weight_flag,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.WMS_LICENSE_PLATE_NUMBERS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(LPN_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(LPN_ID, 0) AS LPN_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.WMS_LICENSE_PLATE_NUMBERS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        LPN_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.WMS_LICENSE_PLATE_NUMBERS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.WMS_LICENSE_PLATE_NUMBERS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    LPN_ID
  ) IN (
    SELECT
      LPN_ID
    FROM bec_raw_dl_ext.WMS_LICENSE_PLATE_NUMBERS
    WHERE
      (LPN_ID, KCA_SEQ_ID) IN (
        SELECT
          LPN_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.WMS_LICENSE_PLATE_NUMBERS
        GROUP BY
          LPN_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'wms_license_plate_numbers';