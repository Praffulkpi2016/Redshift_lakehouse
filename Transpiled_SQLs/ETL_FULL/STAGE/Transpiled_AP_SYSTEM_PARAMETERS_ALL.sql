DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_SYSTEM_PARAMETERS_ALL;
CREATE TABLE bronze_bec_ods_stg.AP_SYSTEM_PARAMETERS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AP_SYSTEM_PARAMETERS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (org_id, last_update_date) IN (
    SELECT
      org_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_SYSTEM_PARAMETERS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      org_id
  );