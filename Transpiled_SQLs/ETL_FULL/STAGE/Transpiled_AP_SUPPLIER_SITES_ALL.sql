DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_SUPPLIER_SITES_ALL;
CREATE TABLE bronze_bec_ods_stg.AP_SUPPLIER_SITES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AP_SUPPLIER_SITES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (vendor_site_id, last_update_date) IN (
    SELECT
      vendor_site_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_SUPPLIER_SITES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      vendor_site_id
  );