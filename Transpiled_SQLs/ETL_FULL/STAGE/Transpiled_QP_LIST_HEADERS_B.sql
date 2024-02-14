DROP TABLE IF EXISTS bronze_bec_ods_stg.QP_LIST_HEADERS_B;
CREATE TABLE bronze_bec_ods_stg.QP_LIST_HEADERS_B AS
SELECT
  *
FROM bec_raw_dl_ext.QP_LIST_HEADERS_B
WHERE
  kca_operation <> 'DELETE'
  AND (LIST_HEADER_ID, last_update_date) IN (
    SELECT
      LIST_HEADER_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.QP_LIST_HEADERS_B
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      LIST_HEADER_ID
  );