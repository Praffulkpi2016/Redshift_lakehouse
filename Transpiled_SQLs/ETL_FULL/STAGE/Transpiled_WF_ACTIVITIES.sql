DROP table IF EXISTS bronze_bec_ods_stg.WF_ACTIVITIES;
CREATE TABLE bronze_bec_ods_stg.WF_ACTIVITIES AS
SELECT
  *
FROM bec_raw_dl_ext.WF_ACTIVITIES
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(ITEM_TYPE, 'NA'), COALESCE(NAME, 'NA'), COALESCE(VERSION, 0)) IN (
    SELECT
      COALESCE(ITEM_TYPE, 'NA') AS ITEM_TYPE,
      COALESCE(NAME, 'NA') AS NAME,
      COALESCE(VERSION, 0) AS VERSION
    FROM bec_raw_dl_ext.WF_ACTIVITIES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(ITEM_TYPE, 'NA'),
      COALESCE(NAME, 'NA'),
      COALESCE(VERSION, 0)
  );