DROP table IF EXISTS bronze_bec_ods_stg.MTL_UNIT_TRANSACTIONS;
CREATE TABLE bronze_bec_ods_stg.MTL_UNIT_TRANSACTIONS AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_UNIT_TRANSACTIONS
WHERE
  kca_operation <> 'DELETE'
  AND (TRANSACTION_ID, SERIAL_NUMBER, last_update_date) IN (
    SELECT
      TRANSACTION_ID,
      SERIAL_NUMBER,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_UNIT_TRANSACTIONS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      TRANSACTION_ID,
      SERIAL_NUMBER
  );