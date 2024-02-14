/* Delete Records */
DELETE FROM silver_bec_ods.MRP_SR_ASSIGNMENTS
WHERE
  COALESCE(ASSIGNMENT_ID, 0) IN (
    SELECT
      COALESCE(stg.ASSIGNMENT_ID, 0) AS ASSIGNMENT_ID
    FROM silver_bec_ods.MRP_SR_ASSIGNMENTS AS ods, bronze_bec_ods_stg.MRP_SR_ASSIGNMENTS AS stg
    WHERE
      COALESCE(ods.ASSIGNMENT_ID, 0) = COALESCE(stg.ASSIGNMENT_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MRP_SR_ASSIGNMENTS (
  assignment_id,
  assignment_type,
  sourcing_rule_id,
  sourcing_rule_type,
  assignment_set_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  organization_id,
  customer_id,
  ship_to_site_id,
  category_id,
  category_set_id,
  inventory_item_id,
  secondary_inventory,
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
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    assignment_id,
    assignment_type,
    sourcing_rule_id,
    sourcing_rule_type,
    assignment_set_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    organization_id,
    customer_id,
    ship_to_site_id,
    category_id,
    category_set_id,
    inventory_item_id,
    secondary_inventory,
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
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MRP_SR_ASSIGNMENTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ASSIGNMENT_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ASSIGNMENT_ID, 0) AS ASSIGNMENT_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MRP_SR_ASSIGNMENTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ASSIGNMENT_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MRP_SR_ASSIGNMENTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MRP_SR_ASSIGNMENTS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    ASSIGNMENT_ID
  ) IN (
    SELECT
      ASSIGNMENT_ID
    FROM bec_raw_dl_ext.MRP_SR_ASSIGNMENTS
    WHERE
      (ASSIGNMENT_ID, KCA_SEQ_ID) IN (
        SELECT
          ASSIGNMENT_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MRP_SR_ASSIGNMENTS
        GROUP BY
          ASSIGNMENT_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mrp_sr_assignments';