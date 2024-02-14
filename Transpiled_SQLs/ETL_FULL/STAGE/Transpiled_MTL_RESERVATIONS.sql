DROP TABLE IF EXISTS bronze_bec_ods_stg.mtl_reservations;
CREATE TABLE bronze_bec_ods_stg.mtl_reservations AS
SELECT
  *
FROM bec_raw_dl_ext.mtl_reservations
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(RESERVATION_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(RESERVATION_ID, 0) AS RESERVATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.mtl_reservations
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(RESERVATION_ID, 0)
  );