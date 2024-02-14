DROP TABLE IF EXISTS bronze_bec_ods_stg.CE_PAYMENT_DOCUMENTS;
CREATE TABLE bronze_bec_ods_stg.CE_PAYMENT_DOCUMENTS AS
SELECT
  *
FROM bec_raw_dl_ext.CE_PAYMENT_DOCUMENTS
WHERE
  kca_operation <> 'DELETE'
  AND (PAYMENT_DOCUMENT_ID, last_update_date) IN (
    SELECT
      PAYMENT_DOCUMENT_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.CE_PAYMENT_DOCUMENTS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      PAYMENT_DOCUMENT_ID
  );