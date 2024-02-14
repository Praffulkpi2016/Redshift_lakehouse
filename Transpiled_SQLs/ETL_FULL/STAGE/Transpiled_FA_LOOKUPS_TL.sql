DROP TABLE IF EXISTS bronze_bec_ods_stg.FA_LOOKUPS_TL;
CREATE TABLE bronze_bec_ods_stg.FA_LOOKUPS_TL AS
SELECT
  *
FROM bec_raw_dl_ext.FA_LOOKUPS_TL
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(LOOKUP_TYPE, 'NA'), COALESCE(LOOKUP_CODE, 'NA'), COALESCE(language, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(LOOKUP_TYPE, 'NA') AS LOOKUP_TYPE,
      COALESCE(LOOKUP_CODE, 'NA') AS LOOKUP_CODE,
      COALESCE(language, 'NA') AS language,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.FA_LOOKUPS_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(LOOKUP_TYPE, 'NA'),
      COALESCE(LOOKUP_CODE, 'NA'),
      COALESCE(language, 'NA')
  );