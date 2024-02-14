DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_CHECKS_ALL;
CREATE TABLE bronze_bec_ods_stg.AP_CHECKS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AP_CHECKS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (check_id, last_update_date) IN (
    SELECT
      check_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_CHECKS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      check_id
  );