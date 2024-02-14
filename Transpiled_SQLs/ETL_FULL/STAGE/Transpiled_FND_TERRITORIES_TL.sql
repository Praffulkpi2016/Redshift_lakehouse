DROP TABLE IF EXISTS bronze_bec_ods_stg.FND_TERRITORIES_TL;
CREATE TABLE bronze_bec_ods_stg.FND_TERRITORIES_TL AS
SELECT
  *
FROM bec_raw_dl_ext.FND_TERRITORIES_TL
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(TERRITORY_CODE, 'NA'), COALESCE(LANGUAGE, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(TERRITORY_CODE, 'NA') AS TERRITORY_CODE,
      COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.FND_TERRITORIES_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(TERRITORY_CODE, 'NA'),
      COALESCE(LANGUAGE, 'NA')
  );