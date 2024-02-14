/* Delete Records */
DELETE FROM silver_bec_ods.MTL_ABC_ASSIGNMENT_GROUPS
WHERE
  (
    COALESCE(ASSIGNMENT_GROUP_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.ASSIGNMENT_GROUP_ID, 0)
    FROM silver_bec_ods.MTL_ABC_ASSIGNMENT_GROUPS AS ods, bronze_bec_ods_stg.MTL_ABC_ASSIGNMENT_GROUPS AS stg
    WHERE
      COALESCE(ods.ASSIGNMENT_GROUP_ID, 0) = COALESCE(stg.ASSIGNMENT_GROUP_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_ABC_ASSIGNMENT_GROUPS (
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
  IS_DELETED_FLG,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_ABC_ASSIGNMENT_GROUPS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ASSIGNMENT_GROUP_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(ASSIGNMENT_GROUP_ID, 0) AS ASSIGNMENT_GROUP_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.MTL_ABC_ASSIGNMENT_GROUPS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ASSIGNMENT_GROUP_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_ABC_ASSIGNMENT_GROUPS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_ABC_ASSIGNMENT_GROUPS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    ASSIGNMENT_GROUP_ID
  ) IN (
    SELECT
      ASSIGNMENT_GROUP_ID
    FROM bec_raw_dl_ext.MTL_ABC_ASSIGNMENT_GROUPS
    WHERE
      (ASSIGNMENT_GROUP_ID, KCA_SEQ_ID) IN (
        SELECT
          ASSIGNMENT_GROUP_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_ABC_ASSIGNMENT_GROUPS
        GROUP BY
          ASSIGNMENT_GROUP_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_abc_assignment_groups';