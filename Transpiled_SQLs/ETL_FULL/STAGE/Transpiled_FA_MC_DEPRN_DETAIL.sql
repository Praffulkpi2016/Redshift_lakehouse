DROP table IF EXISTS bronze_bec_ods_stg.fa_mc_deprn_detail;
CREATE TABLE bronze_bec_ods_stg.fa_mc_deprn_detail AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.fa_mc_deprn_detail
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(ASSET_ID, 0), COALESCE(PERIOD_COUNTER, 0), COALESCE(DISTRIBUTION_ID, 0), COALESCE(SET_OF_BOOKS_ID, 0)) IN (
      SELECT
        COALESCE(BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
        COALESCE(ASSET_ID, 0) AS ASSET_ID,
        COALESCE(PERIOD_COUNTER, 0) AS PERIOD_COUNTER,
        COALESCE(DISTRIBUTION_ID, 0) AS DISTRIBUTION_ID,
        COALESCE(SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID
      FROM bec_raw_dl_ext.fa_mc_deprn_detail
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(BOOK_TYPE_CODE, 'NA'),
        COALESCE(ASSET_ID, 0),
        COALESCE(PERIOD_COUNTER, 0),
        COALESCE(DISTRIBUTION_ID, 0),
        COALESCE(SET_OF_BOOKS_ID, 0)
    )
);