DROP TABLE IF EXISTS bronze_bec_ods_stg.fa_distribution_history;
CREATE TABLE bronze_bec_ods_stg.fa_distribution_history AS
SELECT
  *
FROM bec_raw_dl_ext.fa_distribution_history
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(DISTRIBUTION_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(DISTRIBUTION_ID, 0) AS DISTRIBUTION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.fa_distribution_history
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(DISTRIBUTION_ID, 0)
  );