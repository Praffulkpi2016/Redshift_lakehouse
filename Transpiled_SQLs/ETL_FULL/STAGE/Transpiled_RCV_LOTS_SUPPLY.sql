DROP TABLE IF EXISTS bronze_bec_ods_stg.RCV_LOTS_SUPPLY;
CREATE TABLE bronze_bec_ods_stg.RCV_LOTS_SUPPLY AS
SELECT
  *
FROM bec_raw_dl_ext.RCV_LOTS_SUPPLY
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(SUPPLY_TYPE_CODE, ''), COALESCE(SHIPMENT_LINE_ID, 0), COALESCE(LOT_NUM, ''), quantity, last_update_date) IN (
    SELECT
      COALESCE(SUPPLY_TYPE_CODE, ''),
      COALESCE(SHIPMENT_LINE_ID, 0),
      COALESCE(LOT_NUM, ''),
      quantity,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.RCV_LOTS_SUPPLY
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(SUPPLY_TYPE_CODE, ''),
      COALESCE(SHIPMENT_LINE_ID, 0),
      COALESCE(LOT_NUM, ''),
      quantity
  );