TRUNCATE table bronze_bec_ods_stg.PO_APPROVED_SUPPLIER_LIST;
INSERT INTO bronze_bec_ods_stg.PO_APPROVED_SUPPLIER_LIST (
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
  vendor_id,
  item_id,
  category_id,
  vendor_site_id,
  primary_vendor_item,
  manufacturer_asl_id,
  disable_flag,
  kca_operation,
  kca_seq_id,
  KCA_SEQ_DATE
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
    vendor_id,
    item_id,
    category_id,
    vendor_site_id,
    primary_vendor_item,
    manufacturer_asl_id,
    disable_flag,
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.PO_APPROVED_SUPPLIER_LIST
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (ASL_ID, kca_seq_id) IN (
      SELECT
        ASL_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.PO_APPROVED_SUPPLIER_LIST
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        ASL_ID
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'po_approved_supplier_list'
      )
    )
);