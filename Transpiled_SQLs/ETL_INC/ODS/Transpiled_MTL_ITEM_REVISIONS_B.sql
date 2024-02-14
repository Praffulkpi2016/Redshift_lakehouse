/* Delete Records */
DELETE FROM silver_bec_ods.MTL_ITEM_REVISIONS_B
WHERE
  (REVISION_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID) IN (
    SELECT
      stg.REVISION_ID,
      stg.INVENTORY_ITEM_ID,
      stg.ORGANIZATION_ID
    FROM silver_bec_ods.MTL_ITEM_REVISIONS_B AS ods, bronze_bec_ods_stg.MTL_ITEM_REVISIONS_B AS stg
    WHERE
      ods.REVISION_ID = stg.REVISION_ID
      AND ods.INVENTORY_ITEM_ID = stg.INVENTORY_ITEM_ID
      AND ods.ORGANIZATION_ID = stg.ORGANIZATION_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_ITEM_REVISIONS_B (
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
  is_deleted_flg,
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
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_ITEM_REVISIONS_B
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (REVISION_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, kca_seq_id) IN (
      SELECT
        REVISION_ID,
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MTL_ITEM_REVISIONS_B
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        REVISION_ID,
        INVENTORY_ITEM_ID,
        ORGANIZATION_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_ITEM_REVISIONS_B SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_ITEM_REVISIONS_B SET IS_DELETED_FLG = 'Y'
WHERE
  (REVISION_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID) IN (
    SELECT
      REVISION_ID,
      INVENTORY_ITEM_ID,
      ORGANIZATION_ID
    FROM bec_raw_dl_ext.MTL_ITEM_REVISIONS_B
    WHERE
      (REVISION_ID, INVENTORY_ITEM_ID, ORGANIZATION_ID, KCA_SEQ_ID) IN (
        SELECT
          REVISION_ID,
          INVENTORY_ITEM_ID,
          ORGANIZATION_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_ITEM_REVISIONS_B
        GROUP BY
          REVISION_ID,
          INVENTORY_ITEM_ID,
          ORGANIZATION_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_item_revisions_b';