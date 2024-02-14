DROP TABLE IF EXISTS bronze_bec_ods_stg.AR_CASH_RECEIPT_HISTORY_ALL;
CREATE TABLE bronze_bec_ods_stg.AR_CASH_RECEIPT_HISTORY_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AR_CASH_RECEIPT_HISTORY_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (CASH_RECEIPT_HISTORY_ID, last_update_date) IN (
    SELECT
      CASH_RECEIPT_HISTORY_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AR_CASH_RECEIPT_HISTORY_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      CASH_RECEIPT_HISTORY_ID
  );