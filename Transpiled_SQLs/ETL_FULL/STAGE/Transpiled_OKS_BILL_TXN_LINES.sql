DROP TABLE IF EXISTS bronze_bec_ods_stg.OKS_BILL_TXN_LINES;
CREATE TABLE bronze_bec_ods_stg.oks_bill_txn_lines AS
SELECT
  *
FROM bec_raw_dl_ext.oks_bill_txn_lines
WHERE
  kca_operation <> 'DELETE'
  AND (ID, last_update_date) IN (
    SELECT
      ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.oks_bill_txn_lines
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      ID
  );