DROP TABLE IF EXISTS bronze_bec_ods_stg.FND_LOOKUP_VALUES;
CREATE TABLE bronze_bec_ods_stg.FND_LOOKUP_VALUES AS
SELECT
  *
FROM bec_raw_dl_ext.FND_LOOKUP_VALUES
WHERE
  kca_operation <> 'DELETE'
  AND (LOOKUP_TYPE, LANGUAGE, LOOKUP_CODE, SECURITY_GROUP_ID, VIEW_APPLICATION_ID, last_update_date) IN (
    SELECT
      LOOKUP_TYPE,
      LANGUAGE,
      LOOKUP_CODE,
      SECURITY_GROUP_ID,
      VIEW_APPLICATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.FND_LOOKUP_VALUES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      LOOKUP_TYPE,
      LANGUAGE,
      LOOKUP_CODE,
      SECURITY_GROUP_ID,
      VIEW_APPLICATION_ID
  );