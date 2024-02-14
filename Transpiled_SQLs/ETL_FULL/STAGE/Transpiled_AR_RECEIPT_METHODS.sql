DROP TABLE IF EXISTS bronze_bec_ods_stg.AR_RECEIPT_METHODS;
CREATE TABLE bronze_bec_ods_stg.AR_RECEIPT_METHODS AS
SELECT
  *
FROM bec_raw_dl_ext.AR_RECEIPT_METHODS
WHERE
  kca_operation <> 'DELETE'
  AND (RECEIPT_METHOD_ID, last_update_date) IN (
    SELECT
      RECEIPT_METHOD_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AR_RECEIPT_METHODS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      RECEIPT_METHOD_ID
  );