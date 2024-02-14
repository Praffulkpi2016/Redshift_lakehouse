DROP TABLE IF EXISTS bronze_bec_ods_stg.RA_BATCH_SOURCES_ALL;
CREATE TABLE bronze_bec_ods_stg.RA_BATCH_SOURCES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.RA_BATCH_SOURCES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(batch_source_id, 0), COALESCE(ORG_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(batch_source_id, 0),
      COALESCE(ORG_ID, 0),
      MAX(last_update_date)
    FROM bec_raw_dl_ext.RA_BATCH_SOURCES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(batch_source_id, 0),
      COALESCE(ORG_ID, 0)
  );