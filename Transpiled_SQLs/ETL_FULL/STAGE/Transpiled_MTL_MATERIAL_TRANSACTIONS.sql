DROP table IF EXISTS bronze_bec_ods_stg.MTL_MATERIAL_TRANSACTIONS;
CREATE TABLE bronze_bec_ods_stg.MTL_MATERIAL_TRANSACTIONS AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_MATERIAL_TRANSACTIONS
WHERE
  kca_operation <> 'DELETE'
  AND (TRANSACTION_ID, last_update_date, kca_seq_id) IN (
    SELECT
      TRANSACTION_ID,
      MAX(last_update_date),
      MAX(kca_seq_id)
    FROM bec_raw_dl_ext.MTL_MATERIAL_TRANSACTIONS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      TRANSACTION_ID
  );