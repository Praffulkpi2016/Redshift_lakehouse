DROP TABLE IF EXISTS bronze_bec_ods_stg.bom_departments;
CREATE TABLE bronze_bec_ods_stg.bom_departments AS
SELECT
  *
FROM bec_raw_dl_ext.bom_departments
WHERE
  kca_operation <> 'DELETE'
  AND (DEPARTMENT_ID, last_update_date) IN (
    SELECT
      DEPARTMENT_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.bom_departments
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      DEPARTMENT_ID
  );