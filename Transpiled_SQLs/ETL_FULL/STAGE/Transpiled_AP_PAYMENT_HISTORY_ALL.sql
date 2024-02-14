DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_PAYMENT_HISTORY_ALL;
CREATE TABLE bronze_bec_ods_stg.AP_PAYMENT_HISTORY_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AP_PAYMENT_HISTORY_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (payment_history_id, last_update_date) IN (
    SELECT
      payment_history_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_PAYMENT_HISTORY_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      payment_history_id
  );