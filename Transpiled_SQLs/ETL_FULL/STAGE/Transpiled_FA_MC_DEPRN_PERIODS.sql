DROP TABLE IF EXISTS bronze_bec_ods_stg.fa_mc_deprn_periods;
CREATE TABLE bronze_bec_ods_stg.fa_mc_deprn_periods AS
SELECT
  *
FROM bec_raw_dl_ext.fa_mc_deprn_periods
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(PERIOD_COUNTER, 0), COALESCE(SET_OF_BOOKS_ID, 0)) IN (
    SELECT
      COALESCE(BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
      COALESCE(PERIOD_COUNTER, 0) AS PERIOD_COUNTER,
      COALESCE(SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID
    FROM bec_raw_dl_ext.fa_mc_deprn_periods
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(BOOK_TYPE_CODE, 'NA'),
      COALESCE(PERIOD_COUNTER, 0),
      COALESCE(SET_OF_BOOKS_ID, 0)
  );