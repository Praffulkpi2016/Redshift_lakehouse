DROP TABLE IF EXISTS bronze_bec_ods_stg.HZ_CUST_PROFILE_AMTS;
CREATE TABLE bronze_bec_ods_stg.HZ_CUST_PROFILE_AMTS AS
SELECT
  *
FROM bec_raw_dl_ext.HZ_CUST_PROFILE_AMTS
WHERE
  kca_operation <> 'DELETE'
  AND (CUST_ACCT_PROFILE_AMT_ID, last_update_date) IN (
    SELECT
      CUST_ACCT_PROFILE_AMT_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HZ_CUST_PROFILE_AMTS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      CUST_ACCT_PROFILE_AMT_ID
  );