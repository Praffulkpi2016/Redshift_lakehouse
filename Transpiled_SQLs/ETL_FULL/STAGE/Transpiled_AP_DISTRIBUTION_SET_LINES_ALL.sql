DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_DISTRIBUTION_SET_LINES_ALL;
CREATE TABLE bronze_bec_ods_stg.AP_DISTRIBUTION_SET_LINES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AP_DISTRIBUTION_SET_LINES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (distribution_set_id, distribution_set_line_number, last_update_date) IN (
    SELECT
      distribution_set_id,
      distribution_set_line_number,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_DISTRIBUTION_SET_LINES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      distribution_set_id,
      distribution_set_line_number
  );