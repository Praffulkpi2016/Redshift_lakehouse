DROP table IF EXISTS bronze_bec_ods_stg.FUN_SEQ_VERSIONS;
CREATE TABLE bronze_bec_ods_stg.FUN_SEQ_VERSIONS AS
SELECT
  *
FROM bec_raw_dl_ext.FUN_SEQ_VERSIONS
WHERE
  kca_operation <> 'DELETE'
  AND (SEQ_VERSION_ID, last_update_date, kca_seq_id) IN (
    SELECT
      SEQ_VERSION_ID,
      MAX(last_update_date),
      MAX(kca_seq_id)
    FROM bec_raw_dl_ext.FUN_SEQ_VERSIONS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      SEQ_VERSION_ID
  );