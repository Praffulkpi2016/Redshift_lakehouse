DROP table IF EXISTS bronze_bec_ods_stg.MTL_UNITS_OF_MEASURE_TL;
CREATE TABLE bronze_bec_ods_stg.MTL_UNITS_OF_MEASURE_TL AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_UNITS_OF_MEASURE_TL
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(UNIT_OF_MEASURE, 'NA'), COALESCE(language, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(UNIT_OF_MEASURE, 'NA') AS UNIT_OF_MEASURE,
      COALESCE(language, 'NA') AS language,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_UNITS_OF_MEASURE_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(UNIT_OF_MEASURE, 'NA'),
      COALESCE(language, 'NA')
  );