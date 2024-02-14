DROP TABLE IF EXISTS bronze_bec_ods_stg.RCV_SHIPMENT_LINES;
CREATE TABLE bronze_bec_ods_stg.RCV_SHIPMENT_LINES AS
SELECT
  *
FROM bec_raw_dl_ext.RCV_SHIPMENT_LINES
WHERE
  kca_operation <> 'DELETE'
  AND (SHIPMENT_LINE_ID, last_update_date) IN (
    SELECT
      SHIPMENT_LINE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.RCV_SHIPMENT_LINES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      SHIPMENT_LINE_ID
  );