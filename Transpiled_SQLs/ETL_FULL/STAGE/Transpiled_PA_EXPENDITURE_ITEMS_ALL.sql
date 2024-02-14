DROP TABLE IF EXISTS bronze_bec_ods_stg.pa_expenditure_items_all;
CREATE TABLE bronze_bec_ods_stg.pa_expenditure_items_all AS
SELECT
  *
FROM bec_raw_dl_ext.pa_expenditure_items_all
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(EXPENDITURE_ITEM_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(EXPENDITURE_ITEM_ID, 0) AS EXPENDITURE_ITEM_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.pa_expenditure_items_all
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(EXPENDITURE_ITEM_ID, 0)
  );