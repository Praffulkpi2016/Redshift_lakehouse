DROP table IF EXISTS bronze_bec_ods_stg.IBY_EXT_BANK_ACCOUNTS;
CREATE TABLE bronze_bec_ods_stg.IBY_EXT_BANK_ACCOUNTS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.IBY_EXT_BANK_ACCOUNTS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(EXT_BANK_ACCOUNT_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(EXT_BANK_ACCOUNT_ID, 0) AS EXT_BANK_ACCOUNT_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.IBY_EXT_BANK_ACCOUNTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        EXT_BANK_ACCOUNT_ID
    )
);