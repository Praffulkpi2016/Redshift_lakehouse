DROP TABLE IF EXISTS bronze_bec_ods_stg.HZ_LOCATIONS;
CREATE TABLE bronze_bec_ods_stg.HZ_LOCATIONS AS
SELECT
  *
FROM bec_raw_dl_ext.HZ_LOCATIONS
WHERE
  kca_operation <> 'DELETE'
  AND (LOCATION_ID, last_update_date) IN (
    SELECT
      LOCATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HZ_LOCATIONS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      LOCATION_ID
  );