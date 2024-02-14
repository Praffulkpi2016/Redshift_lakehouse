DROP TABLE IF EXISTS bronze_bec_ods_stg.FND_APPLICATION_TL;
CREATE TABLE bronze_bec_ods_stg.FND_APPLICATION_TL AS
SELECT
  *
FROM bec_raw_dl_ext.FND_APPLICATION_TL
WHERE
  kca_operation <> 'DELETE'
  AND (APPLICATION_ID, LANGUAGE, last_update_date) IN (
    SELECT
      APPLICATION_ID,
      LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.FND_APPLICATION_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      APPLICATION_ID,
      LANGUAGE
  );