DROP TABLE IF EXISTS bronze_bec_ods_stg.HZ_CUST_ACCOUNTS;
CREATE TABLE bronze_bec_ods_stg.HZ_CUST_ACCOUNTS AS
SELECT
  *
FROM bec_raw_dl_ext.HZ_CUST_ACCOUNTS
WHERE
  kca_operation <> 'DELETE'
  AND (CUST_ACCOUNT_ID, last_update_date) IN (
    SELECT
      CUST_ACCOUNT_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HZ_CUST_ACCOUNTS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      CUST_ACCOUNT_ID
  );