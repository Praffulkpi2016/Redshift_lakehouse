DROP TABLE IF EXISTS bronze_bec_ods_stg.pa_project_accum_commitments;
CREATE TABLE bronze_bec_ods_stg.pa_project_accum_commitments AS
SELECT
  *
FROM bec_raw_dl_ext.pa_project_accum_commitments
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(PROJECT_ACCUM_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(PROJECT_ACCUM_ID, 0) AS PROJECT_ACCUM_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.pa_project_accum_commitments
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(PROJECT_ACCUM_ID, 0)
  );