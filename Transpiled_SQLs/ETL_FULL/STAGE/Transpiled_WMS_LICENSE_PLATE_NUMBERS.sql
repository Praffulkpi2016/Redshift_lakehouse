DROP table IF EXISTS bronze_bec_ods_stg.WMS_LICENSE_PLATE_NUMBERS;
CREATE TABLE bronze_bec_ods_stg.WMS_LICENSE_PLATE_NUMBERS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.WMS_LICENSE_PLATE_NUMBERS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(LPN_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(LPN_ID, 0) AS LPN_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.WMS_LICENSE_PLATE_NUMBERS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        LPN_ID
    )
);