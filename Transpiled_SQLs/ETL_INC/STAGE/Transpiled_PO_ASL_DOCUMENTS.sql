TRUNCATE table bronze_bec_ods_stg.PO_ASL_DOCUMENTS;
INSERT INTO bronze_bec_ods_stg.PO_ASL_DOCUMENTS (
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
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.PO_ASL_DOCUMENTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ASL_ID, 0), COALESCE(USING_ORGANIZATION_ID, 0), COALESCE(DOCUMENT_HEADER_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(ASL_ID, 0) AS ASL_ID,
        COALESCE(USING_ORGANIZATION_ID, 0) AS USING_ORGANIZATION_ID,
        COALESCE(DOCUMENT_HEADER_ID, 0) AS DOCUMENT_HEADER_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.PO_ASL_DOCUMENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ASL_ID, 0),
        COALESCE(USING_ORGANIZATION_ID, 0),
        COALESCE(DOCUMENT_HEADER_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'po_asl_documents'
    )
);