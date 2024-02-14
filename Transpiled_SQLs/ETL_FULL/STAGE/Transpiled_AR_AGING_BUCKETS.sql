DROP TABLE IF EXISTS bronze_bec_ods_stg.AR_AGING_BUCKETS;
CREATE TABLE bronze_bec_ods_stg.AR_AGING_BUCKETS AS
SELECT
  *
FROM bec_raw_dl_ext.AR_AGING_BUCKETS
WHERE
  kca_operation <> 'DELETE'
  AND (aging_bucket_id, last_update_date) IN (
    SELECT
      aging_bucket_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AR_AGING_BUCKETS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      aging_bucket_id
  );