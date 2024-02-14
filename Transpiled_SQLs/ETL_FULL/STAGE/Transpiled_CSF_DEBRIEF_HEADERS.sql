DROP TABLE IF EXISTS bronze_bec_ods_stg.CSF_DEBRIEF_HEADERS;
CREATE TABLE bronze_bec_ods_stg.CSF_DEBRIEF_HEADERS AS
SELECT
  *
FROM bec_raw_dl_ext.CSF_DEBRIEF_HEADERS
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(DEBRIEF_HEADER_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(DEBRIEF_HEADER_ID, 0) AS DEBRIEF_HEADER_ID,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.CSF_DEBRIEF_HEADERS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(DEBRIEF_HEADER_ID, 0)
  );