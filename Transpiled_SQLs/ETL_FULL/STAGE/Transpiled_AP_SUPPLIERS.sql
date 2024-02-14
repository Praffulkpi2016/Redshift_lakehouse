DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_SUPPLIERS;
CREATE TABLE bronze_bec_ods_stg.AP_SUPPLIERS AS
SELECT
  *
FROM bec_raw_dl_ext.AP_SUPPLIERS
WHERE
  kca_operation <> 'DELETE'
  AND (vendor_id, last_update_date) IN (
    SELECT
      vendor_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_SUPPLIERS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      vendor_id
  );