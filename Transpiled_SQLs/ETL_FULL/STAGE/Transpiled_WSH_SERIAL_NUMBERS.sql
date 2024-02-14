DROP TABLE IF EXISTS bronze_bec_ods_stg.WSH_SERIAL_NUMBERS;
CREATE TABLE bronze_bec_ods_stg.WSH_SERIAL_NUMBERS AS
SELECT
  *
FROM bec_raw_dl_ext.wsh_serial_numbers
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(DELIVERY_DETAIL_ID, 0), COALESCE(FM_SERIAL_NUMBER, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(DELIVERY_DETAIL_ID, 0) AS DELIVERY_DETAIL_ID,
      COALESCE(FM_SERIAL_NUMBER, 'NA') AS FM_SERIAL_NUMBER,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.wsh_serial_numbers
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(DELIVERY_DETAIL_ID, 0),
      COALESCE(FM_SERIAL_NUMBER, 'NA')
  );