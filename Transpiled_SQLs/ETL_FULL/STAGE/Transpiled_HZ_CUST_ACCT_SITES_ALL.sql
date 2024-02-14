DROP TABLE IF EXISTS bronze_bec_ods_stg.HZ_CUST_ACCT_SITES_ALL;
CREATE TABLE bronze_bec_ods_stg.HZ_CUST_ACCT_SITES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.HZ_CUST_ACCT_SITES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (CUST_ACCT_SITE_ID, last_update_date) IN (
    SELECT
      CUST_ACCT_SITE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HZ_CUST_ACCT_SITES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      CUST_ACCT_SITE_ID
  );