DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_INVOICES_ALL;
CREATE TABLE bronze_bec_ods_stg.AP_INVOICES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AP_INVOICES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (INVOICE_ID, last_update_date) IN (
    SELECT
      INVOICE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_INVOICES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      INVOICE_ID
  );