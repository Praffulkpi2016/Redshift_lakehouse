/* Delete Records */
DELETE FROM silver_bec_ods.PO_ASL_STATUSES
WHERE
  (
    COALESCE(STATUS_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.STATUS_ID, 0) AS STATUS_ID
    FROM silver_bec_ods.PO_ASL_STATUSES AS ods, bronze_bec_ods_stg.PO_ASL_STATUSES AS stg
    WHERE
      COALESCE(ods.STATUS_ID, 0) = COALESCE(stg.STATUS_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.PO_ASL_STATUSES (
  status_id,
  status,
  status_description,
  asl_default_flag,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  inactive_date,
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
  zd_edition_name,
  zd_sync,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    status_id,
    status,
    status_description,
    asl_default_flag,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    inactive_date,
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
    zd_edition_name,
    zd_sync,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.PO_ASL_STATUSES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(STATUS_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(STATUS_ID, 0) AS STATUS_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.PO_ASL_STATUSES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(STATUS_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.PO_ASL_STATUSES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.PO_ASL_STATUSES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    STATUS_ID
  ) IN (
    SELECT
      STATUS_ID
    FROM bec_raw_dl_ext.PO_ASL_STATUSES
    WHERE
      (STATUS_ID, KCA_SEQ_ID) IN (
        SELECT
          STATUS_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.PO_ASL_STATUSES
        GROUP BY
          STATUS_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_asl_statuses';