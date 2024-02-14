DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_REQ_DISTRIBUTIONS_ALL;
CREATE TABLE bronze_bec_ods_stg.PO_REQ_DISTRIBUTIONS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.PO_REQ_DISTRIBUTIONS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (DISTRIBUTION_ID, last_update_date) IN (
    SELECT
      DISTRIBUTION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PO_REQ_DISTRIBUTIONS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      DISTRIBUTION_ID
  );