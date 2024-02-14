DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_DISTRIBUTION_SETS_ALL;
CREATE TABLE bronze_bec_ods_stg.AP_DISTRIBUTION_SETS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AP_DISTRIBUTION_SETS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (distribution_set_id, last_update_date) IN (
    SELECT
      distribution_set_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_DISTRIBUTION_SETS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      distribution_set_id
  );