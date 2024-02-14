DROP TABLE IF EXISTS bronze_bec_ods_stg.fa_retirements;
CREATE TABLE bronze_bec_ods_stg.fa_retirements AS
SELECT
  *
FROM bec_raw_dl_ext.fa_retirements
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(RETIREMENT_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(RETIREMENT_ID, 0) AS RETIREMENT_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.fa_retirements
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(RETIREMENT_ID, 0)
  );