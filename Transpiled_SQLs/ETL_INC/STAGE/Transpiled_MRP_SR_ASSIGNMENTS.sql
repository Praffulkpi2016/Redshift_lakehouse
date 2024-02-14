TRUNCATE table
	table bronze_bec_ods_stg.MRP_SR_ASSIGNMENTS;
INSERT INTO bronze_bec_ods_stg.MRP_SR_ASSIGNMENTS (
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
  KCA_OPERATION,
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
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MRP_SR_ASSIGNMENTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ASSIGNMENT_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ASSIGNMENT_ID, 0) AS ASSIGNMENT_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.MRP_SR_ASSIGNMENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ASSIGNMENT_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'mrp_sr_assignments'
    )
);