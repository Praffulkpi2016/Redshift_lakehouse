DROP TABLE IF EXISTS bronze_bec_ods_stg.HZ_CUST_ACCOUNT_ROLES;
CREATE TABLE bronze_bec_ods_stg.HZ_CUST_ACCOUNT_ROLES AS
SELECT
  *
FROM bec_raw_dl_ext.HZ_CUST_ACCOUNT_ROLES
WHERE
  kca_operation <> 'DELETE'
  AND (CUST_ACCOUNT_ROLE_ID, last_update_date) IN (
    SELECT
      CUST_ACCOUNT_ROLE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HZ_CUST_ACCOUNT_ROLES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      CUST_ACCOUNT_ROLE_ID
  );