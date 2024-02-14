TRUNCATE table bronze_bec_ods_stg.MRP_SOURCING_RULES;
INSERT INTO bronze_bec_ods_stg.MRP_SOURCING_RULES (
  sourcing_rule_id,
  sourcing_rule_name,
  organization_id,
  description,
  status,
  sourcing_rule_type,
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
  planning_active,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    sourcing_rule_id,
    sourcing_rule_name,
    organization_id,
    description,
    status,
    sourcing_rule_type,
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
    planning_active,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MRP_SOURCING_RULES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (SOURCING_RULE_ID, kca_seq_id) IN (
      SELECT
        SOURCING_RULE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MRP_SOURCING_RULES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        SOURCING_RULE_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'mrp_sourcing_rules'
    )
);