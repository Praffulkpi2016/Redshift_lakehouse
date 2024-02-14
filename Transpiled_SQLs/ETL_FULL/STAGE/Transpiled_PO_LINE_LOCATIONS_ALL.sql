DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_LINE_LOCATIONS_ALL;
CREATE TABLE bronze_bec_ods_stg.PO_LINE_LOCATIONS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.PO_LINE_LOCATIONS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (LINE_LOCATION_ID, last_update_date) IN (
    SELECT
      LINE_LOCATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PO_LINE_LOCATIONS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      LINE_LOCATION_ID
  );