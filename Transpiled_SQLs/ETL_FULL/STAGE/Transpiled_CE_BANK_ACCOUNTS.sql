DROP TABLE IF EXISTS bronze_bec_ods_stg.CE_BANK_ACCOUNTS;
CREATE TABLE bronze_bec_ods_stg.CE_BANK_ACCOUNTS AS
SELECT
  *
FROM bec_raw_dl_ext.CE_BANK_ACCOUNTS
WHERE
  kca_operation <> 'DELETE'
  AND (BANK_ACCOUNT_ID, last_update_date) IN (
    SELECT
      BANK_ACCOUNT_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.CE_BANK_ACCOUNTS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      BANK_ACCOUNT_ID
  );