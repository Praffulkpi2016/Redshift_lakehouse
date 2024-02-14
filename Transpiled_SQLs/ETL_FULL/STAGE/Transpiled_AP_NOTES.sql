DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_NOTES;
CREATE TABLE bronze_bec_ods_stg.AP_NOTES AS
SELECT
  *
FROM bec_raw_dl_ext.AP_NOTES
WHERE
  kca_operation <> 'DELETE'
  AND (NOTE_ID, last_update_date) IN (
    SELECT
      NOTE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_NOTES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      NOTE_ID
  );