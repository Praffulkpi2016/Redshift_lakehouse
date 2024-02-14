DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_PAYMENT_SCHEDULES_ALL;
CREATE TABLE bronze_bec_ods_stg.AP_PAYMENT_SCHEDULES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AP_PAYMENT_SCHEDULES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (INVOICE_ID, PAYMENT_NUM, last_update_date) IN (
    SELECT
      INVOICE_ID,
      PAYMENT_NUM,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_PAYMENT_SCHEDULES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      INVOICE_ID,
      PAYMENT_NUM
  );