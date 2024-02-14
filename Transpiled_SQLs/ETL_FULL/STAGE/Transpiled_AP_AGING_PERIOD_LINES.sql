DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_AGING_PERIOD_LINES;
CREATE TABLE bronze_bec_ods_stg.AP_AGING_PERIOD_LINES AS
SELECT
  *
FROM bec_raw_dl_ext.AP_AGING_PERIOD_LINES
WHERE
  kca_operation <> 'DELETE'
  AND (aging_period_line_id, last_update_date) IN (
    SELECT
      aging_period_line_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_AGING_PERIOD_LINES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      aging_period_line_id
  );