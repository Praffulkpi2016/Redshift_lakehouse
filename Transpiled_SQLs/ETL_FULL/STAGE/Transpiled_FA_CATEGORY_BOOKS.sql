DROP TABLE IF EXISTS bronze_bec_ods_stg.fa_category_books;
CREATE TABLE bronze_bec_ods_stg.fa_category_books AS
SELECT
  *
FROM bec_raw_dl_ext.fa_category_books
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(CATEGORY_ID, 0), COALESCE(BOOK_TYPE_CODE, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(CATEGORY_ID, 0) AS CATEGORY_ID,
      COALESCE(BOOK_TYPE_CODE, 'NA') AS BOOK_TYPE_CODE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.fa_category_books
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(CATEGORY_ID, 0),
      COALESCE(BOOK_TYPE_CODE, 'NA')
  );