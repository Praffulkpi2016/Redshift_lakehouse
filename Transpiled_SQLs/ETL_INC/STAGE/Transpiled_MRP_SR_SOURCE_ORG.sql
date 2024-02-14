TRUNCATE table bronze_bec_ods_stg.MRP_SR_SOURCE_ORG;
INSERT INTO bronze_bec_ods_stg.MRP_SR_SOURCE_ORG (
  sr_source_id,
  sr_receipt_id,
  source_organization_id,
  vendor_id,
  vendor_site_id,
  secondary_inventory,
  source_type,
  allocation_percent,
  `rank`,
  old_rank,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
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
  ship_method,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    sr_source_id,
    sr_receipt_id,
    source_organization_id,
    vendor_id,
    vendor_site_id,
    secondary_inventory,
    source_type,
    allocation_percent,
    `rank`,
    old_rank,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
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
    ship_method,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MRP_SR_SOURCE_ORG
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (SR_SOURCE_ID, kca_seq_id) IN (
      SELECT
        SR_SOURCE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MRP_SR_SOURCE_ORG
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        SR_SOURCE_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'mrp_sr_source_org'
    )
);