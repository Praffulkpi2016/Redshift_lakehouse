DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_CHECK_FORMATS;
CREATE TABLE bronze_bec_ods_stg.AP_CHECK_FORMATS AS
SELECT
  *
FROM bec_raw_dl_ext.AP_CHECK_FORMATS
WHERE
  kca_operation <> 'DELETE'
  AND (check_format_id, last_update_date) IN (
    SELECT
      check_format_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_CHECK_FORMATS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      check_format_id
  );