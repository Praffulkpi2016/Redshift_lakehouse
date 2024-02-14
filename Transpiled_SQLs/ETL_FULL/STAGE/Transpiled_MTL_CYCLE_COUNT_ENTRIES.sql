DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_CYCLE_COUNT_ENTRIES;
CREATE TABLE bronze_bec_ods_stg.MTL_CYCLE_COUNT_ENTRIES AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_CYCLE_COUNT_ENTRIES
WHERE
  kca_operation <> 'DELETE'
  AND (CYCLE_COUNT_ENTRY_ID, last_update_date) IN (
    SELECT
      CYCLE_COUNT_ENTRY_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_CYCLE_COUNT_ENTRIES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      CYCLE_COUNT_ENTRY_ID
  );