DROP TABLE IF EXISTS bronze_bec_ods_stg.RCV_SHIPMENT_HEADERS;
CREATE TABLE bronze_bec_ods_stg.RCV_SHIPMENT_HEADERS AS
SELECT
  *
FROM bec_raw_dl_ext.RCV_SHIPMENT_HEADERS
WHERE
  kca_operation <> 'DELETE'
  AND (SHIPMENT_HEADER_ID, last_update_date) IN (
    SELECT
      SHIPMENT_HEADER_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.RCV_SHIPMENT_HEADERS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      SHIPMENT_HEADER_ID
  );