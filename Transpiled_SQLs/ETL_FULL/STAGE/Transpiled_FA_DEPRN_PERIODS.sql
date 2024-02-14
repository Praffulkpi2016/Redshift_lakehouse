DROP table IF EXISTS bronze_bec_ods_stg.fa_deprn_periods;
CREATE TABLE bronze_bec_ods_stg.fa_deprn_periods AS
SELECT
  *
FROM bec_raw_dl_ext.fa_deprn_periods
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(PERIOD_COUNTER, 0)) IN (
    SELECT
      COALESCE(BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
      COALESCE(PERIOD_COUNTER, 0) AS PERIOD_COUNTER
    FROM bec_raw_dl_ext.fa_deprn_periods
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(BOOK_TYPE_CODE, 'NA'),
      COALESCE(PERIOD_COUNTER, 0)
  );