DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_INCOME_TAX_REGIONS;
CREATE TABLE bronze_bec_ods_stg.AP_INCOME_TAX_REGIONS AS
SELECT
  *
FROM bec_raw_dl_ext.AP_INCOME_TAX_REGIONS
WHERE
  kca_operation <> 'DELETE'
  AND (REGION_SHORT_NAME, last_update_date) IN (
    SELECT
      REGION_SHORT_NAME,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_INCOME_TAX_REGIONS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      REGION_SHORT_NAME
  );