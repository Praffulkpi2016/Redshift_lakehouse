DROP TABLE IF EXISTS bronze_bec_ods_stg.CS_BILLING_TYPE_CATEGORIES;
CREATE TABLE bronze_bec_ods_stg.CS_BILLING_TYPE_CATEGORIES AS
SELECT
  *
FROM bec_raw_dl_ext.CS_BILLING_TYPE_CATEGORIES
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(BILLING_TYPE, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(BILLING_TYPE, 'NA') AS BILLING_TYPE,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.CS_BILLING_TYPE_CATEGORIES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(BILLING_TYPE, 'NA')
  );