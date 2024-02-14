DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_INCOME_TAX_TYPES;
CREATE TABLE bronze_bec_ods_stg.AP_INCOME_TAX_TYPES AS
SELECT
  *
FROM bec_raw_dl_ext.AP_INCOME_TAX_TYPES
WHERE
  kca_operation <> 'DELETE'
  AND (income_tax_type, last_update_date) IN (
    SELECT
      income_tax_type,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_INCOME_TAX_TYPES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      income_tax_type
  );