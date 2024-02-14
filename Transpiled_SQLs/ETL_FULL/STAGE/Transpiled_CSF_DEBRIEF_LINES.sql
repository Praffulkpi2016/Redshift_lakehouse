DROP TABLE IF EXISTS bronze_bec_ods_stg.CSF_DEBRIEF_LINES;
CREATE TABLE bronze_bec_ods_stg.CSF_DEBRIEF_LINES AS
SELECT
  *
FROM bec_raw_dl_ext.CSF_DEBRIEF_LINES
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(DEBRIEF_LINE_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(DEBRIEF_LINE_ID, 0) AS DEBRIEF_LINE_ID,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.CSF_DEBRIEF_LINES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(DEBRIEF_LINE_ID, 0)
  );