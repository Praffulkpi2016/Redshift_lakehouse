DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_EXPENSE_REPORT_HEADERS_ALL;
CREATE TABLE bronze_bec_ods_stg.AP_EXPENSE_REPORT_HEADERS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AP_EXPENSE_REPORT_HEADERS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (REPORT_HEADER_ID, last_update_date) IN (
    SELECT
      REPORT_HEADER_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_EXPENSE_REPORT_HEADERS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      REPORT_HEADER_ID
  );