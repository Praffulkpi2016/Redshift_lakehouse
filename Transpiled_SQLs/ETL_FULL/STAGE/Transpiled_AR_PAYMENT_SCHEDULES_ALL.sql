DROP TABLE IF EXISTS bronze_bec_ods_stg.AR_PAYMENT_SCHEDULES_ALL;
CREATE TABLE bronze_bec_ods_stg.AR_PAYMENT_SCHEDULES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AR_PAYMENT_SCHEDULES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (PAYMENT_SCHEDULE_ID, last_update_date) IN (
    SELECT
      PAYMENT_SCHEDULE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AR_PAYMENT_SCHEDULES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      PAYMENT_SCHEDULE_ID
  );