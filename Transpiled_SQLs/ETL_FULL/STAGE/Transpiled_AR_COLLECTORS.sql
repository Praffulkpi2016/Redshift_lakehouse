DROP TABLE IF EXISTS bronze_bec_ods_stg.AR_COLLECTORS;
CREATE TABLE bronze_bec_ods_stg.AR_COLLECTORS AS
SELECT
  *
FROM bec_raw_dl_ext.AR_COLLECTORS
WHERE
  kca_operation <> 'DELETE'
  AND (COLLECTOR_ID, last_update_date) IN (
    SELECT
      COLLECTOR_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AR_COLLECTORS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COLLECTOR_ID
  );