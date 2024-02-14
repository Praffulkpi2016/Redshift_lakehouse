DROP TABLE IF EXISTS bronze_bec_ods_stg.OE_LINES_IFACE_ALL;
CREATE TABLE bronze_bec_ods_stg.OE_LINES_IFACE_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.OE_LINES_IFACE_ALL
WHERE
  kca_operation <> 'DELETE';