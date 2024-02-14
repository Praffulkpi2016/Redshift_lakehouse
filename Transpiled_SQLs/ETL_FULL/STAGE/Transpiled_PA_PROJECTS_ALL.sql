DROP TABLE IF EXISTS bronze_bec_ods_stg.PA_PROJECTS_ALL;
CREATE TABLE bronze_bec_ods_stg.PA_PROJECTS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.PA_PROJECTS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (PROJECT_ID, last_update_date) IN (
    SELECT
      PROJECT_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PA_PROJECTS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      PROJECT_ID
  );