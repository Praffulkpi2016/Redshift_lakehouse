TRUNCATE table bronze_bec_ods_stg.MTL_ABC_ASSIGNMENT_GROUPS;
INSERT INTO bronze_bec_ods_stg.MTL_ABC_ASSIGNMENT_GROUPS (
  assignment_group_id,
  assignment_group_name,
  organization_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  compile_id,
  secondary_inventory,
  item_scope_type,
  classification_method_type,
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
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    assignment_group_id,
    assignment_group_name,
    organization_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    compile_id,
    secondary_inventory,
    item_scope_type,
    classification_method_type,
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
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_ABC_ASSIGNMENT_GROUPS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ASSIGNMENT_GROUP_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(ASSIGNMENT_GROUP_ID, 0) AS ASSIGNMENT_GROUP_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MTL_ABC_ASSIGNMENT_GROUPS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ASSIGNMENT_GROUP_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'mtl_abc_assignment_groups'
    )
);