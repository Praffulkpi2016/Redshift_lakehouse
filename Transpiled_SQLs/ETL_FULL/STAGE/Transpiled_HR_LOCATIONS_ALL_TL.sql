DROP TABLE IF EXISTS bronze_bec_ods_stg.HR_LOCATIONS_ALL_TL;
CREATE TABLE bronze_bec_ods_stg.HR_LOCATIONS_ALL_TL AS
SELECT
  *
FROM bec_raw_dl_ext.HR_LOCATIONS_ALL_TL
WHERE
  kca_operation <> 'DELETE'
  AND (LOCATION_ID, LANGUAGE, last_update_date) IN (
    SELECT
      LOCATION_ID,
      LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HR_LOCATIONS_ALL_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      LOCATION_ID,
      LANGUAGE
  );