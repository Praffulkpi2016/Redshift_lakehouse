DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_HOLDS_ALL;
CREATE TABLE bronze_bec_ods_stg.AP_HOLDS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AP_HOLDS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (HOLD_ID, last_update_date) IN (
    SELECT
      HOLD_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_HOLDS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      HOLD_ID
  );