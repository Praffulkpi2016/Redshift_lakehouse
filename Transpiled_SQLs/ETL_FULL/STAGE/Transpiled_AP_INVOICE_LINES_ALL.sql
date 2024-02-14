DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_INVOICE_LINES_ALL;
CREATE TABLE bronze_bec_ods_stg.AP_INVOICE_LINES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AP_INVOICE_LINES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (invoice_id, line_number, last_update_date) IN (
    SELECT
      invoice_id,
      line_number,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_INVOICE_LINES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      invoice_id,
      line_number
  );