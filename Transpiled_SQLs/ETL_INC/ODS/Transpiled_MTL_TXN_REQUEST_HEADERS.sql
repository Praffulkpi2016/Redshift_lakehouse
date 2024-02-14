/* Delete Records */
DELETE FROM silver_bec_ods.MTL_TXN_REQUEST_HEADERS
WHERE
  (
    COALESCE(HEADER_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.HEADER_ID, 0) AS HEADER_ID
    FROM silver_bec_ods.MTL_TXN_REQUEST_HEADERS AS ods, bronze_bec_ods_stg.MTL_TXN_REQUEST_HEADERS AS stg
    WHERE
      COALESCE(ods.HEADER_ID, 0) = COALESCE(stg.HEADER_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_TXN_REQUEST_HEADERS (
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
  IS_DELETED_FLG,
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_TXN_REQUEST_HEADERS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(HEADER_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(HEADER_ID, 0) AS HEADER_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MTL_TXN_REQUEST_HEADERS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(HEADER_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_TXN_REQUEST_HEADERS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_TXN_REQUEST_HEADERS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    HEADER_ID
  ) IN (
    SELECT
      HEADER_ID
    FROM bec_raw_dl_ext.MTL_TXN_REQUEST_HEADERS
    WHERE
      (HEADER_ID, KCA_SEQ_ID) IN (
        SELECT
          HEADER_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_TXN_REQUEST_HEADERS
        GROUP BY
          HEADER_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_txn_request_headers';