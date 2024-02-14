DROP TABLE IF EXISTS bronze_bec_ods_stg.mtl_txn_request_lines;
CREATE TABLE bronze_bec_ods_stg.mtl_txn_request_lines AS
SELECT
  *
FROM bec_raw_dl_ext.mtl_txn_request_lines
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(LINE_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(LINE_ID, 0) AS LINE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.mtl_txn_request_lines
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(LINE_ID, 0)
  );