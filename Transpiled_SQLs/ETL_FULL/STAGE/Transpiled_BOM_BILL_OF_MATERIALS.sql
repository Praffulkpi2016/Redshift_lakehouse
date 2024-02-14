DROP table IF EXISTS bronze_bec_ods_stg.bom_bill_of_materials;
CREATE TABLE bronze_bec_ods_stg.bom_bill_of_materials AS
SELECT
  *
FROM bec_raw_dl_ext.bom_bill_of_materials
WHERE
  kca_operation <> 'DELETE';