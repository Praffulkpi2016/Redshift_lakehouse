DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_TXN_REQUEST_HEADERS;
CREATE TABLE bronze_bec_ods_stg.MTL_TXN_REQUEST_HEADERS AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_TXN_REQUEST_HEADERS
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(HEADER_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(HEADER_ID, 0) AS HEADER_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_TXN_REQUEST_HEADERS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(HEADER_ID, 0)
  );