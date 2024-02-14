DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_LINES_ALL;
CREATE TABLE bronze_bec_ods_stg.PO_LINES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.PO_LINES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (PO_LINE_ID, last_update_date) IN (
    SELECT
      PO_LINE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PO_LINES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      PO_LINE_ID
  );