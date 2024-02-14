DROP TABLE IF EXISTS bronze_bec_ods_stg.CST_ACTIVITIES;
CREATE TABLE bronze_bec_ods_stg.CST_ACTIVITIES AS
SELECT
  *
FROM bec_raw_dl_ext.CST_ACTIVITIES
WHERE
  kca_operation <> 'DELETE'
  AND (ACTIVITY_ID, last_update_date) IN (
    SELECT
      ACTIVITY_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.CST_ACTIVITIES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      ACTIVITY_ID
  );