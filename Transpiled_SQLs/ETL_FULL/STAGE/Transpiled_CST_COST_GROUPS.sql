DROP TABLE IF EXISTS bronze_bec_ods_stg.CST_COST_GROUPS;
CREATE TABLE bronze_bec_ods_stg.CST_COST_GROUPS AS
SELECT
  *
FROM bec_raw_dl_ext.CST_COST_GROUPS
WHERE
  kca_operation <> 'DELETE'
  AND (COST_GROUP_ID, last_update_date) IN (
    SELECT
      COST_GROUP_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.CST_COST_GROUPS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COST_GROUP_ID
  );