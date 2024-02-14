/* Delete Records */
DELETE FROM silver_bec_ods.MRP_SR_SOURCE_ORG
WHERE
  COALESCE(SR_SOURCE_ID, 0) IN (
    SELECT
      COALESCE(stg.SR_SOURCE_ID, 0)
    FROM silver_bec_ods.MRP_SR_SOURCE_ORG AS ods, bronze_bec_ods_stg.MRP_SR_SOURCE_ORG AS stg
    WHERE
      ods.SR_SOURCE_ID = stg.SR_SOURCE_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MRP_SR_SOURCE_ORG (
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
  IS_DELETED_FLG,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MRP_SR_SOURCE_ORG
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (SR_SOURCE_ID, kca_seq_id) IN (
      SELECT
        SR_SOURCE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MRP_SR_SOURCE_ORG
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        SR_SOURCE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MRP_SR_SOURCE_ORG SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MRP_SR_SOURCE_ORG SET IS_DELETED_FLG = 'Y'
WHERE
  (
    SR_SOURCE_ID
  ) IN (
    SELECT
      SR_SOURCE_ID
    FROM bec_raw_dl_ext.MRP_SR_SOURCE_ORG
    WHERE
      (SR_SOURCE_ID, KCA_SEQ_ID) IN (
        SELECT
          SR_SOURCE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MRP_SR_SOURCE_ORG
        GROUP BY
          SR_SOURCE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mrp_sr_source_org';