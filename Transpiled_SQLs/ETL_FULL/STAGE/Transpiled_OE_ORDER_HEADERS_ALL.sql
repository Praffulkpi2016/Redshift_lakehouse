DROP TABLE IF EXISTS bronze_bec_ods_stg.OE_ORDER_HEADERS_ALL;
CREATE TABLE bronze_bec_ods_stg.OE_ORDER_HEADERS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.OE_ORDER_HEADERS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (HEADER_ID, last_update_date) IN (
    SELECT
      HEADER_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.OE_ORDER_HEADERS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      HEADER_ID
  );