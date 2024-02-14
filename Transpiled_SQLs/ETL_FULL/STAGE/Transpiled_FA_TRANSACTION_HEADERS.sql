DROP TABLE IF EXISTS bronze_bec_ods_stg.fa_transaction_headers;
CREATE TABLE bronze_bec_ods_stg.fa_transaction_headers AS
SELECT
  *
FROM bec_raw_dl_ext.fa_transaction_headers
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(TRANSACTION_HEADER_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(TRANSACTION_HEADER_ID, 0),
      MAX(last_update_date)
    FROM bec_raw_dl_ext.fa_transaction_headers
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(TRANSACTION_HEADER_ID, 0)
  );