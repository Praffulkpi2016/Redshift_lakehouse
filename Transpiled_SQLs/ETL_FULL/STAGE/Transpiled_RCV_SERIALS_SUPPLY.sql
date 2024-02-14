DROP TABLE IF EXISTS bronze_bec_ods_stg.RCV_SERIALS_SUPPLY;
CREATE TABLE bronze_bec_ods_stg.RCV_SERIALS_SUPPLY AS
SELECT
  *
FROM bec_raw_dl_ext.RCV_SERIALS_SUPPLY
WHERE
  kca_operation <> 'DELETE'
  AND (SHIPMENT_LINE_ID, SERIAL_NUM, last_update_date) IN (
    SELECT
      SHIPMENT_LINE_ID,
      SERIAL_NUM,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.RCV_SERIALS_SUPPLY
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      SHIPMENT_LINE_ID,
      SERIAL_NUM
  );