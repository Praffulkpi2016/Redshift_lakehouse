DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_CATEGORY_SETS_TL;
CREATE TABLE bronze_bec_ods_stg.MTL_CATEGORY_SETS_TL AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_CATEGORY_SETS_TL
WHERE
  kca_operation <> 'DELETE'
  AND (CATEGORY_SET_ID, LANGUAGE, last_update_date) IN (
    SELECT
      CATEGORY_SET_ID,
      LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_CATEGORY_SETS_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      CATEGORY_SET_ID,
      LANGUAGE
  );