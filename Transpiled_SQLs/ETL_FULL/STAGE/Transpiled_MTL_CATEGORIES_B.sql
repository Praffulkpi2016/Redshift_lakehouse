DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_CATEGORIES_B;
CREATE TABLE bronze_bec_ods_stg.MTL_CATEGORIES_B AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_CATEGORIES_B
WHERE
  kca_operation <> 'DELETE'
  AND (category_id, last_update_date) IN (
    SELECT
      category_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_CATEGORIES_B
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      category_id
  );