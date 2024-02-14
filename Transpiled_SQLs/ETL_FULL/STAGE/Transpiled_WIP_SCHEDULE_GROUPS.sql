DROP table IF EXISTS bronze_bec_ods_stg.WIP_SCHEDULE_GROUPS;
CREATE TABLE bronze_bec_ods_stg.WIP_SCHEDULE_GROUPS AS
SELECT
  *
FROM bec_raw_dl_ext.WIP_SCHEDULE_GROUPS
WHERE
  kca_operation <> 'DELETE'
  AND (SCHEDULE_GROUP_ID, last_update_date) IN (
    SELECT
      SCHEDULE_GROUP_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.WIP_SCHEDULE_GROUPS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      SCHEDULE_GROUP_ID
  );