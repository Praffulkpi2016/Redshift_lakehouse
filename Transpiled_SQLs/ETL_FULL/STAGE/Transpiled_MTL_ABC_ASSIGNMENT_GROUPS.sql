DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_ABC_ASSIGNMENT_GROUPS;
CREATE TABLE bronze_bec_ods_stg.MTL_ABC_ASSIGNMENT_GROUPS AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_ABC_ASSIGNMENT_GROUPS
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(ASSIGNMENT_GROUP_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(ASSIGNMENT_GROUP_ID, 0) AS ASSIGNMENT_GROUP_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_ABC_ASSIGNMENT_GROUPS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(ASSIGNMENT_GROUP_ID, 0)
  );