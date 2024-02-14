DROP TABLE IF EXISTS bronze_bec_ods_stg.CHV_BUCKET_PATTERNS;
CREATE TABLE bronze_bec_ods_stg.CHV_BUCKET_PATTERNS AS
SELECT
  *
FROM bec_raw_dl_ext.CHV_BUCKET_PATTERNS
WHERE
  kca_operation <> 'DELETE'
  AND (BUCKET_PATTERN_ID, last_update_date) IN (
    SELECT
      COALESCE(BUCKET_PATTERN_ID, 0) AS BUCKET_PATTERN_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.CHV_BUCKET_PATTERNS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(BUCKET_PATTERN_ID, 0)
  );