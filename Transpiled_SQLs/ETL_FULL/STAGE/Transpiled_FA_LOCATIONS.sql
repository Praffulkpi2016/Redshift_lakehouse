DROP table IF EXISTS bronze_bec_ods_stg.fa_locations;
CREATE TABLE bronze_bec_ods_stg.fa_locations AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.fa_locations
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(LOCATION_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(LOCATION_ID, 0) AS LOCATION_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.fa_locations
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(LOCATION_ID, 0)
    )
);