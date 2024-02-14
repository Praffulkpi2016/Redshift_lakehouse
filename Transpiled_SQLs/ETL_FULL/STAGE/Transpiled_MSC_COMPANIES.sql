DROP TABLE IF EXISTS bronze_bec_ods_stg.MSC_COMPANIES;
CREATE TABLE bronze_bec_ods_stg.MSC_COMPANIES AS
SELECT
  *
FROM bec_raw_dl_ext.MSC_COMPANIES
WHERE
  kca_operation <> 'DELETE'
  AND (COMPANY_ID, COMPANY_NAME, last_update_date) IN (
    SELECT
      COMPANY_ID,
      COMPANY_NAME,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MSC_COMPANIES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COMPANY_ID,
      COMPANY_NAME
  );