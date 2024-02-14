DROP table IF EXISTS bronze_bec_ods_stg.PO_ASL_DOCUMENTS;
CREATE TABLE bronze_bec_ods_stg.PO_ASL_DOCUMENTS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.PO_ASL_DOCUMENTS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(ASL_ID, 0), COALESCE(USING_ORGANIZATION_ID, 0), COALESCE(DOCUMENT_HEADER_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(ASL_ID, 0) AS ASL_ID,
        COALESCE(USING_ORGANIZATION_ID, 0) AS USING_ORGANIZATION_ID,
        COALESCE(DOCUMENT_HEADER_ID, 0) AS DOCUMENT_HEADER_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.PO_ASL_DOCUMENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(ASL_ID, 0),
        COALESCE(USING_ORGANIZATION_ID, 0),
        COALESCE(DOCUMENT_HEADER_ID, 0)
    )
);