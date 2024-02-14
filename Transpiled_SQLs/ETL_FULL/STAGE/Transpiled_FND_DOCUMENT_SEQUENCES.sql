DROP TABLE IF EXISTS bronze_bec_ods_stg.fnd_document_sequences;
CREATE TABLE bronze_bec_ods_stg.fnd_document_sequences AS
SELECT
  *
FROM bec_raw_dl_ext.fnd_document_sequences
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(DOC_SEQUENCE_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(DOC_SEQUENCE_ID, 0),
      MAX(last_update_date)
    FROM bec_raw_dl_ext.fnd_document_sequences
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(DOC_SEQUENCE_ID, 0)
  );