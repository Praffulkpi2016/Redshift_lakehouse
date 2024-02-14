TRUNCATE table bronze_bec_ods_stg.BOM_STRUCTURES_B;
INSERT INTO bronze_bec_ods_stg.bom_structures_b (
  assembly_item_id,
  organization_id,
  alternate_bom_designator,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  common_assembly_item_id,
  specific_assembly_comment,
  pending_from_ecn,
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
  assembly_type,
  common_bill_sequence_id,
  bill_sequence_id,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  common_organization_id,
  next_explode_date,
  project_id,
  task_id,
  original_system_reference,
  structure_type_id,
  implementation_date,
  obj_name,
  pk1_value,
  pk2_value,
  pk3_value,
  pk4_value,
  pk5_value,
  effectivity_control,
  is_preferred,
  source_bill_sequence_id,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    assembly_item_id,
    organization_id,
    alternate_bom_designator,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    common_assembly_item_id,
    specific_assembly_comment,
    pending_from_ecn,
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
    assembly_type,
    common_bill_sequence_id,
    bill_sequence_id,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    common_organization_id,
    next_explode_date,
    project_id,
    task_id,
    original_system_reference,
    structure_type_id,
    implementation_date,
    obj_name,
    pk1_value,
    pk2_value,
    pk3_value,
    pk4_value,
    pk5_value,
    effectivity_control,
    is_preferred,
    source_bill_sequence_id,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.bom_structures_b
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (BILL_SEQUENCE_ID, kca_seq_id) IN (
      SELECT
        BILL_SEQUENCE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.bom_structures_b
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        BILL_SEQUENCE_ID
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'bom_structures_b'
    )
);