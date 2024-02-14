DROP table IF EXISTS bronze_bec_ods_stg.QP_PRICELISTS_LOV_V;
CREATE TABLE bronze_bec_ods_stg.QP_PRICELISTS_LOV_V AS
SELECT
  *
FROM bec_raw_dl_ext.QP_PRICELISTS_LOV_V
WHERE
  kca_operation <> 'DELETE';