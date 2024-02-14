TRUNCATE table
	table bronze_bec_ods_stg.WMS_LICENSE_PLATE_NUMBERS;
INSERT INTO bronze_bec_ods_stg.WMS_LICENSE_PLATE_NUMBERS (
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
  KCA_OPERATION,
  KCA_SEQ_ID,
  KCA_SEQ_DATE
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
    KCA_OPERATION,
    KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.WMS_LICENSE_PLATE_NUMBERS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(LPN_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(LPN_ID, 0) AS LPN_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.WMS_LICENSE_PLATE_NUMBERS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        LPN_ID
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'wms_license_plate_numbers'
      )
    )
);