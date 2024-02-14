DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_PLANNERS;
CREATE TABLE bronze_bec_ods_stg.MTL_PLANNERS AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_PLANNERS
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(ORGANIZATION_ID, 0), COALESCE(PLANNER_CODE, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(ORGANIZATION_ID, 0) AS ORGANIZATION_ID,
      COALESCE(PLANNER_CODE, 'NA') AS PLANNER_CODE,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.MTL_PLANNERS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(ORGANIZATION_ID, 0),
      COALESCE(PLANNER_CODE, 'NA')
  );