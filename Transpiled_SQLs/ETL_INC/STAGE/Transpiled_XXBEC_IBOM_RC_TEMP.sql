DROP TABLE IF EXISTS bronze_bec_ods_stg.xxbec_ibom_rc_temp;
CREATE TABLE bronze_bec_ods_stg.xxbec_ibom_rc_temp AS
SELECT
  *
FROM bec_raw_dl_ext.xxbec_ibom_rc_temp
WHERE
  kca_operation <> 'DELETE';