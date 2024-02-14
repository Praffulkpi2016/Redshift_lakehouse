DROP TABLE IF EXISTS bronze_bec_ods_stg.mtl_consumption_transactions;
CREATE TABLE bronze_bec_ods_stg.mtl_consumption_transactions AS
SELECT
  *
FROM bec_raw_dl_ext.mtl_consumption_transactions
WHERE
  kca_operation <> 'DELETE'
  AND (TRANSACTION_ID, last_update_date) IN (
    SELECT
      TRANSACTION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.mtl_consumption_transactions
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      TRANSACTION_ID
  );