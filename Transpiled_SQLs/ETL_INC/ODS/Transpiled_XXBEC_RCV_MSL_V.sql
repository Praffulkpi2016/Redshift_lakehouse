/* Delete Records */
DELETE FROM silver_bec_ods.XXBEC_RCV_MSL_V
WHERE
  (ORG_ID, ITEM_ID, SHIPMENT_HEADER_ID) IN (
    SELECT
      stg.ORG_ID,
      stg.ITEM_ID,
      stg.SHIPMENT_HEADER_ID
    FROM silver_bec_ods.XXBEC_RCV_MSL_V AS ods, bronze_bec_ods_stg.XXBEC_RCV_MSL_V AS stg
    WHERE
      ods.ORG_ID = stg.ORG_ID
      AND ods.ITEM_ID = stg.ITEM_ID
      AND ods.SHIPMENT_HEADER_ID = stg.SHIPMENT_HEADER_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.XXBEC_RCV_MSL_V (
  source_type,
  created_by,
  creation_date,
  last_updated_by,
  last_update_date,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
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
  charge_account_id,
  comments,
  deliver_to_location_id,
  deliver_to_person_id,
  destination_context,
  destination_type_code,
  destination_type,
  employee_id,
  from_organization_id,
  item_description,
  item_id,
  item_revision,
  hazard_class,
  un_number,
  line_num,
  item_category_id,
  locator_id,
  need_by_date,
  packing_slip,
  quantity_received,
  quantity_shipped,
  requisition_line_id,
  requisition_header_id,
  order_num,
  order_line_num,
  req_distribution_id,
  shipment_header_id,
  shipment_line_id,
  shipment_line_status_code,
  source_document_code,
  source_document_type,
  to_organization_id,
  to_subinventory,
  transfer_cost,
  transportation_account_id,
  transportation_cost,
  unit_of_measure,
  uom_conversion_rate,
  routing_header_id,
  routing_name,
  reason_id,
  reason_name,
  location_code,
  deliver_to_person,
  po_header_id,
  po_line_id,
  po_line_location_id,
  po_release_id,
  release_num,
  vendor_name,
  vendor_site_code,
  ship_to_location_id,
  primary_unit_of_measure,
  vendor_id,
  bar_code_label,
  truck_num,
  container_num,
  vendor_lot_num,
  secondary_quantity_received,
  secondary_quantity_shipped,
  secondary_unit_of_measure,
  qc_grade,
  org_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    source_type,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
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
    charge_account_id,
    comments,
    deliver_to_location_id,
    deliver_to_person_id,
    destination_context,
    destination_type_code,
    destination_type,
    employee_id,
    from_organization_id,
    item_description,
    item_id,
    item_revision,
    hazard_class,
    un_number,
    line_num,
    item_category_id,
    locator_id,
    need_by_date,
    packing_slip,
    quantity_received,
    quantity_shipped,
    requisition_line_id,
    requisition_header_id,
    order_num,
    order_line_num,
    req_distribution_id,
    shipment_header_id,
    shipment_line_id,
    shipment_line_status_code,
    source_document_code,
    source_document_type,
    to_organization_id,
    to_subinventory,
    transfer_cost,
    transportation_account_id,
    transportation_cost,
    unit_of_measure,
    uom_conversion_rate,
    routing_header_id,
    routing_name,
    reason_id,
    reason_name,
    location_code,
    deliver_to_person,
    po_header_id,
    po_line_id,
    po_line_location_id,
    po_release_id,
    release_num,
    vendor_name,
    vendor_site_code,
    ship_to_location_id,
    primary_unit_of_measure,
    vendor_id,
    bar_code_label,
    truck_num,
    container_num,
    vendor_lot_num,
    secondary_quantity_received,
    secondary_quantity_shipped,
    secondary_unit_of_measure,
    qc_grade,
    org_id,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.XXBEC_RCV_MSL_V
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (ORG_ID, ITEM_ID, SHIPMENT_HEADER_ID, kca_seq_id) IN (
      SELECT
        ORG_ID,
        ITEM_ID,
        SHIPMENT_HEADER_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.XXBEC_RCV_MSL_V
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        ORG_ID,
        ITEM_ID,
        SHIPMENT_HEADER_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.XXBEC_RCV_MSL_V SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.XXBEC_RCV_MSL_V SET IS_DELETED_FLG = 'Y'
WHERE
  (ORG_ID, ITEM_ID, SHIPMENT_HEADER_ID) IN (
    SELECT
      ORG_ID,
      ITEM_ID,
      SHIPMENT_HEADER_ID
    FROM bec_raw_dl_ext.XXBEC_RCV_MSL_V
    WHERE
      (ORG_ID, ITEM_ID, SHIPMENT_HEADER_ID, KCA_SEQ_ID) IN (
        SELECT
          ORG_ID,
          ITEM_ID,
          SHIPMENT_HEADER_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.XXBEC_RCV_MSL_V
        GROUP BY
          ORG_ID,
          ITEM_ID,
          SHIPMENT_HEADER_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'xxbec_rcv_msl_v';