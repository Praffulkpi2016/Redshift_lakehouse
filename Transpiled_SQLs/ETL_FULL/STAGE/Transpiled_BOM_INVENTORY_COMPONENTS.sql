DROP table IF EXISTS bronze_bec_ods_stg.bom_inventory_components;
CREATE TABLE bronze_bec_ods_stg.bom_inventory_components AS
SELECT
  *
FROM bec_raw_dl_ext.bom_inventory_components
WHERE
  kca_operation <> 'DELETE';