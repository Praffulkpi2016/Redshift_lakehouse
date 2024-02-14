DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_APPROVED_SUPPLIER_LIST;
CREATE TABLE bronze_bec_ods_stg.PO_APPROVED_SUPPLIER_LIST AS
SELECT
  *
FROM bec_raw_dl_ext.PO_APPROVED_SUPPLIER_LIST
WHERE
  kca_operation <> 'DELETE'
  AND (ASL_ID, last_update_date) IN (
    SELECT
      ASL_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PO_APPROVED_SUPPLIER_LIST
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      ASL_ID
  );