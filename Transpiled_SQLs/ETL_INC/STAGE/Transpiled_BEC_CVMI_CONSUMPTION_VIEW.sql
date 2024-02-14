DROP TABLE IF EXISTS bronze_bec_ods_stg.BEC_CVMI_CONSUMPTION_VIEW;
CREATE TABLE bronze_bec_ods_stg.BEC_CVMI_CONSUMPTION_VIEW AS
SELECT
  *
FROM bec_raw_dl_ext.BEC_CVMI_CONSUMPTION_VIEW
WHERE
  kca_operation <> 'DELETE';