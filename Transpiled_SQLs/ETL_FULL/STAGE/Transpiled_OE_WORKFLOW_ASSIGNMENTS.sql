DROP TABLE IF EXISTS bronze_bec_ods_stg.oe_workflow_assignments;
CREATE TABLE bronze_bec_ods_stg.oe_workflow_assignments AS
SELECT
  *
FROM bec_raw_dl_ext.oe_workflow_assignments
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(ASSIGNMENT_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(ASSIGNMENT_ID, 0) AS ASSIGNMENT_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.oe_workflow_assignments
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(ASSIGNMENT_ID, 0)
  );