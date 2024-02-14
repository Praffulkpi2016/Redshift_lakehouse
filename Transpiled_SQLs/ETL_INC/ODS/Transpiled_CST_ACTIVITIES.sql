/* Delete Records */
DELETE FROM silver_bec_ods.cst_activities
WHERE
  activity_id IN (
    SELECT
      stg.activity_id
    FROM silver_bec_ods.cst_activities AS ods, bronze_bec_ods_stg.cst_activities AS stg
    WHERE
      ods.activity_id = stg.activity_id AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.cst_activities (
  activity_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  activity,
  organization_id,
  description,
  default_basis_type,
  disable_date,
  output_uom,
  value_added_activity_flag,
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
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    activity_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    activity,
    organization_id,
    description,
    default_basis_type,
    disable_date,
    output_uom,
    value_added_activity_flag,
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
    zd_edition_name,
    zd_sync,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.cst_activities
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (activity_id, kca_seq_id) IN (
      SELECT
        activity_id,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.cst_activities
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        activity_id
    )
);
/* Soft delete */
UPDATE silver_bec_ods.cst_activities SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.cst_activities SET IS_DELETED_FLG = 'Y'
WHERE
  (
    activity_id
  ) IN (
    SELECT
      activity_id
    FROM bec_raw_dl_ext.cst_activities
    WHERE
      (activity_id, KCA_SEQ_ID) IN (
        SELECT
          activity_id,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.cst_activities
        GROUP BY
          activity_id
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'cst_activities';