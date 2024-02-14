DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_HOLD_CODES;
CREATE TABLE bronze_bec_ods_stg.AP_HOLD_CODES AS
SELECT
  *
FROM bec_raw_dl_ext.AP_HOLD_CODES
WHERE
  kca_operation <> 'DELETE'
  AND (hold_lookup_code, last_update_date) IN (
    SELECT
      hold_lookup_code,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_HOLD_CODES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      hold_lookup_code
  );