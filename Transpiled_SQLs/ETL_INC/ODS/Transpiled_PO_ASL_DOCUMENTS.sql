/* Delete Records */
DELETE FROM silver_bec_ods.PO_ASL_DOCUMENTS
WHERE
  (COALESCE(ASL_ID, 0), COALESCE(USING_ORGANIZATION_ID, 0), COALESCE(DOCUMENT_HEADER_ID, 0)) IN (
    SELECT
      COALESCE(stg.ASL_ID, 0) AS ASL_ID,
      COALESCE(stg.USING_ORGANIZATION_ID, 0) AS USING_ORGANIZATION_ID,
      COALESCE(stg.DOCUMENT_HEADER_ID, 0) AS DOCUMENT_HEADER_ID
    FROM silver_bec_ods.PO_ASL_DOCUMENTS AS ods, bronze_bec_ods_stg.PO_ASL_DOCUMENTS AS stg
    WHERE
      COALESCE(ods.ASL_ID, 0) = COALESCE(stg.ASL_ID, 0)
      AND COALESCE(ods.USING_ORGANIZATION_ID, 0) = COALESCE(stg.USING_ORGANIZATION_ID, 0)
      AND COALESCE(ods.DOCUMENT_HEADER_ID, 0) = COALESCE(stg.DOCUMENT_HEADER_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.PO_ASL_DOCUMENTS (
  asl_id,
  using_organization_id,
  sequence_num,
  document_type_code,
  document_header_id,
  document_line_id,
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
  org_id,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    asl_id,
    using_organization_id,
    sequence_num,
    document_type_code,
    document_header_id,
    document_line_id,
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
    org_id,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.PO_ASL_DOCUMENTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ASL_ID, 0), COALESCE(USING_ORGANIZATION_ID, 0), COALESCE(DOCUMENT_HEADER_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(ASL_ID, 0) AS ASL_ID,
        COALESCE(USING_ORGANIZATION_ID, 0) AS USING_ORGANIZATION_ID,
        COALESCE(DOCUMENT_HEADER_ID, 0) AS DOCUMENT_HEADER_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.PO_ASL_DOCUMENTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ASL_ID, 0),
        COALESCE(USING_ORGANIZATION_ID, 0),
        COALESCE(DOCUMENT_HEADER_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.PO_ASL_DOCUMENTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.PO_ASL_DOCUMENTS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(ASL_ID, 0), COALESCE(USING_ORGANIZATION_ID, 0), COALESCE(DOCUMENT_HEADER_ID, 0)) IN (
    SELECT
      COALESCE(ASL_ID, 0),
      COALESCE(USING_ORGANIZATION_ID, 0),
      COALESCE(DOCUMENT_HEADER_ID, 0)
    FROM bec_raw_dl_ext.PO_ASL_DOCUMENTS
    WHERE
      (COALESCE(ASL_ID, 0), COALESCE(USING_ORGANIZATION_ID, 0), COALESCE(DOCUMENT_HEADER_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(ASL_ID, 0),
          COALESCE(USING_ORGANIZATION_ID, 0),
          COALESCE(DOCUMENT_HEADER_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.PO_ASL_DOCUMENTS
        GROUP BY
          COALESCE(ASL_ID, 0),
          COALESCE(USING_ORGANIZATION_ID, 0),
          COALESCE(DOCUMENT_HEADER_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_asl_documents';