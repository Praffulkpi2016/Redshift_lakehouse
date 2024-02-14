DROP table IF EXISTS bronze_bec_ods_stg.MTL_TXN_SOURCE_TYPES;
CREATE TABLE bronze_bec_ods_stg.MTL_TXN_SOURCE_TYPES AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_TXN_SOURCE_TYPES
WHERE
  kca_operation <> 'DELETE'
  AND (TRANSACTION_SOURCE_TYPE_ID, last_update_date) IN (
    SELECT
      TRANSACTION_SOURCE_TYPE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_TXN_SOURCE_TYPES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      TRANSACTION_SOURCE_TYPE_ID
  );