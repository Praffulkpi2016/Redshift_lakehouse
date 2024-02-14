DROP TABLE IF EXISTS bronze_bec_ods_stg.CE_BANK_ACCT_USES_ALL;
CREATE TABLE bronze_bec_ods_stg.CE_BANK_ACCT_USES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.CE_BANK_ACCT_USES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (BANK_ACCT_USE_ID, last_update_date) IN (
    SELECT
      BANK_ACCT_USE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.CE_BANK_ACCT_USES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      BANK_ACCT_USE_ID
  );