/* Delete Records */
DELETE FROM silver_bec_ods.PO_APPROVED_SUPPLIER_LIST
WHERE
  ASL_ID IN (
    SELECT
      stg.ASL_ID
    FROM silver_bec_ods.PO_APPROVED_SUPPLIER_LIST AS ods, bronze_bec_ods_stg.PO_APPROVED_SUPPLIER_LIST AS stg
    WHERE
      ods.ASL_ID = stg.ASL_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.PO_APPROVED_SUPPLIER_LIST (
  asl_id,
  using_organization_id,
  owning_organization_id,
  vendor_business_type,
  asl_status_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  manufacturer_id,
  vendor_id,
  item_id,
  category_id,
  vendor_site_id,
  primary_vendor_item,
  manufacturer_asl_id,
  review_by_date,
  comments,
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
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  disable_flag,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    asl_id,
    using_organization_id,
    owning_organization_id,
    vendor_business_type,
    asl_status_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    manufacturer_id,
    vendor_id,
    item_id,
    category_id,
    vendor_site_id,
    primary_vendor_item,
    manufacturer_asl_id,
    review_by_date,
    comments,
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
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    disable_flag,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.PO_APPROVED_SUPPLIER_LIST
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (ASL_ID, kca_seq_id) IN (
      SELECT
        ASL_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.PO_APPROVED_SUPPLIER_LIST
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        ASL_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.PO_APPROVED_SUPPLIER_LIST SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.PO_APPROVED_SUPPLIER_LIST SET IS_DELETED_FLG = 'Y'
WHERE
  (
    ASL_ID
  ) IN (
    SELECT
      ASL_ID
    FROM bec_raw_dl_ext.PO_APPROVED_SUPPLIER_LIST
    WHERE
      (ASL_ID, KCA_SEQ_ID) IN (
        SELECT
          ASL_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.PO_APPROVED_SUPPLIER_LIST
        GROUP BY
          ASL_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_approved_supplier_list';