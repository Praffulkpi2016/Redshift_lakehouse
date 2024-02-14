DROP TABLE IF EXISTS bronze_bec_ods_stg.XXBEC_PEGGING_DATA;
CREATE TABLE bronze_bec_ods_stg.xxbec_pegging_data AS
SELECT
  *
FROM bec_raw_dl_ext.xxbec_pegging_data
WHERE
  kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = '';