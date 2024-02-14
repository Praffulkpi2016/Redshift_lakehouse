DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_HEADERS_ALL;
CREATE TABLE bronze_bec_ods_stg.PO_HEADERS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.PO_HEADERS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (PO_HEADER_ID, last_update_date) IN (
    SELECT
      PO_HEADER_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PO_HEADERS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      PO_HEADER_ID
  );