DROP TABLE IF EXISTS bronze_bec_ods_stg.JTF_TASK_TYPES_TL;
CREATE TABLE bronze_bec_ods_stg.JTF_TASK_TYPES_TL AS
SELECT
  *
FROM bec_raw_dl_ext.JTF_TASK_TYPES_TL
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(TASK_TYPE_ID, 0), COALESCE(LANGUAGE, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(TASK_TYPE_ID, 0) AS TASK_TYPE_ID,
      COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.JTF_TASK_TYPES_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(TASK_TYPE_ID, 0),
      COALESCE(LANGUAGE, 'NA')
  );