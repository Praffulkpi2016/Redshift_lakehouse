DROP table IF EXISTS bronze_bec_ods_stg.MSC_CALENDAR_DATES;
CREATE TABLE bronze_bec_ods_stg.MSC_CALENDAR_DATES AS
SELECT
  *
FROM bec_raw_dl_ext.MSC_CALENDAR_DATES
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(SR_INSTANCE_ID, 0), COALESCE(CALENDAR_DATE, '1900-01-01 00:00:00'), COALESCE(CALENDAR_CODE, 'NA'), COALESCE(EXCEPTION_SET_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(SR_INSTANCE_ID, 0) AS SR_INSTANCE_ID,
      COALESCE(CALENDAR_DATE, '1900-01-01 00:00:00') AS CALENDAR_DATE,
      COALESCE(CALENDAR_CODE, 'NA') AS CALENDAR_CODE,
      COALESCE(EXCEPTION_SET_ID, 0) AS EXCEPTION_SET_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MSC_CALENDAR_DATES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(SR_INSTANCE_ID, 0),
      COALESCE(CALENDAR_DATE, '1900-01-01 00:00:00'),
      COALESCE(CALENDAR_CODE, 'NA'),
      COALESCE(EXCEPTION_SET_ID, 0)
  );