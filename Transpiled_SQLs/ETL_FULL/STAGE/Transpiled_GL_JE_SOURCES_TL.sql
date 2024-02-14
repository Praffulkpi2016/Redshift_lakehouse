DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_JE_SOURCES_TL;
CREATE TABLE bronze_bec_ods_stg.GL_JE_SOURCES_TL AS
SELECT
  *
FROM bec_raw_dl_ext.GL_JE_SOURCES_TL
WHERE
  kca_operation <> 'DELETE'
  AND (JE_SOURCE_NAME, LANGUAGE, last_update_date) IN (
    SELECT
      JE_SOURCE_NAME,
      LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_JE_SOURCES_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      JE_SOURCE_NAME,
      LANGUAGE
  );