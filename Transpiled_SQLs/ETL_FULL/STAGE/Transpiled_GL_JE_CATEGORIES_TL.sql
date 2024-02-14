DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_JE_CATEGORIES_TL;
CREATE TABLE bronze_bec_ods_stg.GL_JE_CATEGORIES_TL AS
SELECT
  *
FROM bec_raw_dl_ext.GL_JE_CATEGORIES_TL
WHERE
  kca_operation <> 'DELETE'
  AND (JE_CATEGORY_NAME, LANGUAGE, last_update_date) IN (
    SELECT
      JE_CATEGORY_NAME,
      LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_JE_CATEGORIES_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      JE_CATEGORY_NAME,
      LANGUAGE
  );