DROP table IF EXISTS bronze_bec_ods_stg.GL_ACCESS_SET_LEDGERS;
CREATE TABLE bronze_bec_ods_stg.GL_ACCESS_SET_LEDGERS AS
SELECT
  *
FROM bec_raw_dl_ext.GL_ACCESS_SET_LEDGERS
WHERE
  kca_operation <> 'DELETE';