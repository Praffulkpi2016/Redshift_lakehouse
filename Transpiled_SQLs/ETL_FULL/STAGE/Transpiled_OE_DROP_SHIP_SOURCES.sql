DROP TABLE IF EXISTS bronze_bec_ods_stg.OE_DROP_SHIP_SOURCES;
CREATE TABLE bronze_bec_ods_stg.OE_DROP_SHIP_SOURCES AS
SELECT
  *
FROM bec_raw_dl_ext.OE_DROP_SHIP_SOURCES
WHERE
  kca_operation <> 'DELETE'
  AND (HEADER_ID, LINE_ID, last_update_date) IN (
    SELECT
      HEADER_ID,
      LINE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.OE_DROP_SHIP_SOURCES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      HEADER_ID,
      LINE_ID
  );