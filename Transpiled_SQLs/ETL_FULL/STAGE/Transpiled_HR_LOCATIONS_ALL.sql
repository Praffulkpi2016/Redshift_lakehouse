DROP TABLE IF EXISTS bronze_bec_ods_stg.HR_LOCATIONS_ALL;
CREATE TABLE bronze_bec_ods_stg.HR_LOCATIONS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.HR_LOCATIONS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (LOCATION_ID, last_update_date) IN (
    SELECT
      LOCATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HR_LOCATIONS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      LOCATION_ID
  );