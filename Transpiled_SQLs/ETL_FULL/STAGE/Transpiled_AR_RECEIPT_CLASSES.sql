DROP TABLE IF EXISTS bronze_bec_ods_stg.AR_RECEIPT_CLASSES;
CREATE TABLE bronze_bec_ods_stg.AR_RECEIPT_CLASSES AS
SELECT
  *
FROM bec_raw_dl_ext.AR_RECEIPT_CLASSES
WHERE
  kca_operation <> 'DELETE'
  AND (RECEIPT_CLASS_ID, last_update_date) IN (
    SELECT
      RECEIPT_CLASS_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AR_RECEIPT_CLASSES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      RECEIPT_CLASS_ID
  );