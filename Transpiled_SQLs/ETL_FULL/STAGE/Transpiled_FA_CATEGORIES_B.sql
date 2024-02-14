DROP TABLE IF EXISTS bronze_bec_ods_stg.fa_categories_b;
CREATE TABLE bronze_bec_ods_stg.fa_categories_b AS
SELECT
  *
FROM bec_raw_dl_ext.fa_categories_b
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(CATEGORY_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(CATEGORY_ID, 0) AS CATEGORY_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.fa_categories_b
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(CATEGORY_ID, 0)
  );