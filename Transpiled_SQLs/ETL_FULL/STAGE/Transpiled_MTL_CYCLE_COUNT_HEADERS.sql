DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_CYCLE_COUNT_HEADERS;
CREATE TABLE bronze_bec_ods_stg.MTL_CYCLE_COUNT_HEADERS AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_CYCLE_COUNT_HEADERS
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(CYCLE_COUNT_HEADER_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(CYCLE_COUNT_HEADER_ID, 0) AS CYCLE_COUNT_HEADER_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_CYCLE_COUNT_HEADERS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(CYCLE_COUNT_HEADER_ID, 0)
  );