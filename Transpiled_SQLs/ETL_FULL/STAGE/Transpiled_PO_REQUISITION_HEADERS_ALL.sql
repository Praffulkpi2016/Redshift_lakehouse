DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_REQUISITION_HEADERS_ALL;
CREATE TABLE bronze_bec_ods_stg.PO_REQUISITION_HEADERS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.PO_REQUISITION_HEADERS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (REQUISITION_HEADER_ID, last_update_date) IN (
    SELECT
      REQUISITION_HEADER_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PO_REQUISITION_HEADERS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      REQUISITION_HEADER_ID
  );