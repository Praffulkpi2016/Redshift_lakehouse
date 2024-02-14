DROP TABLE IF EXISTS bronze_bec_ods_stg.PA_TASKS;
CREATE TABLE bronze_bec_ods_stg.PA_TASKS AS
SELECT
  *
FROM bec_raw_dl_ext.PA_TASKS
WHERE
  kca_operation <> 'DELETE'
  AND (task_id, last_update_date) IN (
    SELECT
      task_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PA_TASKS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      TASK_ID
  );