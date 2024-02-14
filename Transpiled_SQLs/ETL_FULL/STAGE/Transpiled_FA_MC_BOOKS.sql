DROP table IF EXISTS bronze_bec_ods_stg.fa_mc_books;
CREATE TABLE bronze_bec_ods_stg.fa_mc_books AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.fa_mc_books
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(SET_OF_BOOKS_ID, 0), COALESCE(TRANSACTION_HEADER_ID_IN, 0), last_update_date) IN (
      SELECT
        COALESCE(SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID,
        COALESCE(TRANSACTION_HEADER_ID_IN, 0) AS TRANSACTION_HEADER_ID_IN,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.fa_mc_books
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        COALESCE(SET_OF_BOOKS_ID, 0),
        COALESCE(TRANSACTION_HEADER_ID_IN, 0)
    )
);