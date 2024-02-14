DROP TABLE IF EXISTS bronze_bec_ods_stg.WIP_DISCRETE_JOBS;
CREATE TABLE bronze_bec_ods_stg.WIP_DISCRETE_JOBS AS
SELECT
  *
FROM bec_raw_dl_ext.WIP_DISCRETE_JOBS
WHERE
  kca_operation <> 'DELETE'
  AND (WIP_ENTITY_ID, last_update_date) IN (
    SELECT
      WIP_ENTITY_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.WIP_DISCRETE_JOBS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      WIP_ENTITY_ID
  );