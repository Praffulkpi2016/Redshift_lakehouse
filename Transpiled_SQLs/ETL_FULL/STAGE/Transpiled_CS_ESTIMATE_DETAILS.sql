DROP TABLE IF EXISTS bronze_bec_ods_stg.CS_ESTIMATE_DETAILS;
CREATE TABLE bronze_bec_ods_stg.CS_ESTIMATE_DETAILS AS
SELECT
  *
FROM bec_raw_dl_ext.CS_ESTIMATE_DETAILS
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(ESTIMATE_DETAIL_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(ESTIMATE_DETAIL_ID, 0) AS ESTIMATE_DETAIL_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.CS_ESTIMATE_DETAILS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(ESTIMATE_DETAIL_ID, 0)
  );