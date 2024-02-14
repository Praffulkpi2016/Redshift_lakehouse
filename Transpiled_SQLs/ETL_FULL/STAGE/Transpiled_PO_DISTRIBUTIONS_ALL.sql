DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_DISTRIBUTIONS_ALL;
CREATE TABLE bronze_bec_ods_stg.PO_DISTRIBUTIONS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.PO_DISTRIBUTIONS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (PO_DISTRIBUTION_ID, last_update_date) IN (
    SELECT
      PO_DISTRIBUTION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PO_DISTRIBUTIONS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      PO_DISTRIBUTION_ID
  );