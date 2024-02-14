DROP table IF EXISTS bronze_bec_ods_stg.fa_mc_book_controls;
CREATE TABLE bronze_bec_ods_stg.fa_mc_book_controls AS
SELECT
  *
FROM bec_raw_dl_ext.fa_mc_book_controls
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(BOOK_TYPE_CODE, 'NA'), COALESCE(SET_OF_BOOKS_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
      COALESCE(SET_OF_BOOKS_ID, 0) AS SET_OF_BOOKS_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.fa_mc_book_controls
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(BOOK_TYPE_CODE, 'NA'),
      COALESCE(SET_OF_BOOKS_ID, 0)
  );