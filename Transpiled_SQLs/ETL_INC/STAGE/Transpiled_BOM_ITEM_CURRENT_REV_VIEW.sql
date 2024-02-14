DROP TABLE IF EXISTS bronze_bec_ods_stg.bom_item_current_rev_view;
CREATE TABLE bronze_bec_ods_stg.bom_item_current_rev_view AS
SELECT
  *
FROM bec_raw_dl_ext.bom_item_current_rev_view
WHERE
  kca_operation <> 'DELETE';