DROP TABLE IF EXISTS bronze_bec_ods_stg.JTF_TASK_ASSIGNMENTS;
CREATE TABLE bronze_bec_ods_stg.JTF_TASK_ASSIGNMENTS AS
SELECT
  *
FROM bec_raw_dl_ext.JTF_TASK_ASSIGNMENTS
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(TASK_ASSIGNMENT_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(TASK_ASSIGNMENT_ID, 0) AS TASK_ASSIGNMENT_ID,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.JTF_TASK_ASSIGNMENTS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(TASK_ASSIGNMENT_ID, 0)
  );