DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_BANK_BRANCHES;
CREATE TABLE bronze_bec_ods_stg.AP_BANK_BRANCHES AS
SELECT
  *
FROM bec_raw_dl_ext.AP_BANK_BRANCHES
WHERE
  kca_operation <> 'DELETE'
  AND (bank_branch_id, last_update_date) IN (
    SELECT
      bank_branch_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_BANK_BRANCHES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      bank_branch_id
  );