DROP TABLE IF EXISTS bronze_bec_ods_stg.HZ_CUST_PROFILE_CLASSES;
CREATE TABLE bronze_bec_ods_stg.HZ_CUST_PROFILE_CLASSES AS
SELECT
  *
FROM bec_raw_dl_ext.HZ_CUST_PROFILE_CLASSES
WHERE
  kca_operation <> 'DELETE'
  AND (PROFILE_CLASS_ID, last_update_date) IN (
    SELECT
      PROFILE_CLASS_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HZ_CUST_PROFILE_CLASSES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      PROFILE_CLASS_ID
  );