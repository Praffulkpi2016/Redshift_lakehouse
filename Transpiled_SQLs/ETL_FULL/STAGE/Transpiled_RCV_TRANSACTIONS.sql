DROP TABLE IF EXISTS bronze_bec_ods_stg.RCV_TRANSACTIONS;
CREATE TABLE bronze_bec_ods_stg.RCV_TRANSACTIONS AS
SELECT
  *
FROM bec_raw_dl_ext.RCV_TRANSACTIONS
WHERE
  kca_operation <> 'DELETE'
  AND (TRANSACTION_ID, last_update_date) IN (
    SELECT
      TRANSACTION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.RCV_TRANSACTIONS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      TRANSACTION_ID
  );