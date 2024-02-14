DROP TABLE IF EXISTS bronze_bec_ods_stg.HZ_CUST_SITE_USES_ALL;
CREATE TABLE bronze_bec_ods_stg.HZ_CUST_SITE_USES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.HZ_CUST_SITE_USES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (SITE_USE_ID, last_update_date) IN (
    SELECT
      SITE_USE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HZ_CUST_SITE_USES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      SITE_USE_ID
  );