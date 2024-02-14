TRUNCATE table bronze_bec_ods_stg.MTL_ITEM_REVISIONS_B;
INSERT INTO bronze_bec_ods_stg.MTL_ITEM_REVISIONS_B (
  inventory_item_id,
  organization_id,
  revision,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  change_notice,
  ecn_initiation_date,
  implementation_date,
  implemented_serial_number,
  effectivity_date,
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
  revised_item_sequence_id,
  description,
  object_version_number,
  revision_id,
  revision_label,
  revision_reason,
  lifecycle_id,
  current_phase_id,
  kca_operation,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    inventory_item_id,
    organization_id,
    revision,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    change_notice,
    ecn_initiation_date,
    implementation_date,
    implemented_serial_number,
    effectivity_date,
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
    revised_item_sequence_id,
    description,
    object_version_number,
    revision_id,
    revision_label,
    revision_reason,
    lifecycle_id,
    current_phase_id,
    kca_operation,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_ITEM_REVISIONS_B
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (INVENTORY_ITEM_ID, ORGANIZATION_ID, REVISION_ID, kca_seq_id) IN (
      SELECT
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        REVISION_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.MTL_ITEM_REVISIONS_B
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        REVISION_ID
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - 10000
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_item_revisions_b'
      )
    )
);