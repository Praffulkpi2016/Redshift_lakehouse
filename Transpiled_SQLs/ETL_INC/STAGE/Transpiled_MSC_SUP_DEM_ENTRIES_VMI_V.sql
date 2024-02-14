DROP table IF EXISTS bronze_bec_ods_stg.MSC_SUP_DEM_ENTRIES_VMI_V;
CREATE TABLE bronze_bec_ods_stg.MSC_SUP_DEM_ENTRIES_VMI_V AS
SELECT
  *
FROM bec_raw_dl_ext.MSC_SUP_DEM_ENTRIES_VMI_V
WHERE
  kca_operation <> 'DELETE';