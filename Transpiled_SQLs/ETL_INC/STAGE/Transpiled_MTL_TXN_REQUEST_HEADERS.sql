TRUNCATE table bronze_bec_ods_stg.MTL_TXN_REQUEST_HEADERS;
INSERT INTO bronze_bec_ods_stg.MTL_TXN_REQUEST_HEADERS (
  header_id,
  request_number,
  transaction_type_id,
  move_order_type,
  organization_id,
  description,
  date_required,
  from_subinventory_code,
  to_subinventory_code,
  to_account_id,
  header_status,
  status_date,
  last_updated_by,
  last_update_login,
  last_update_date,
  created_by,
  creation_date,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  grouping_rule_id,
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
  attribute_category,
  ship_to_location_id,
  freight_code,
  shipment_method,
  auto_receipt_flag,
  reference_id,
  reference_detail_id,
  assignment_id,
  kca_operation,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    header_id,
    request_number,
    transaction_type_id,
    move_order_type,
    organization_id,
    description,
    date_required,
    from_subinventory_code,
    to_subinventory_code,
    to_account_id,
    header_status,
    status_date,
    last_updated_by,
    last_update_login,
    last_update_date,
    created_by,
    creation_date,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    grouping_rule_id,
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
    attribute_category,
    ship_to_location_id,
    freight_code,
    shipment_method,
    auto_receipt_flag,
    reference_id,
    reference_detail_id,
    assignment_id,
    kca_operation, /* ZD_EDITION_NAME, */ /* ZD_SYNC, */
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_TXN_REQUEST_HEADERS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(HEADER_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(HEADER_ID, 0) AS HEADER_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MTL_TXN_REQUEST_HEADERS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(HEADER_ID, 0)
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_txn_request_headers'
      )
    )
);