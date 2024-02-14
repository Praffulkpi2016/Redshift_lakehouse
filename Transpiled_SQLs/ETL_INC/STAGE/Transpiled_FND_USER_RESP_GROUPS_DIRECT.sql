DROP TABLE IF EXISTS bronze_bec_ods_stg.fnd_user_resp_groups_direct;
CREATE TABLE bronze_bec_ods_stg.fnd_user_resp_groups_direct AS
SELECT
  *
FROM bec_raw_dl_ext.fnd_user_resp_groups_direct
WHERE
  kca_operation <> 'DELETE';