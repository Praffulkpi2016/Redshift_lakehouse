DROP TABLE IF EXISTS bronze_bec_ods_stg.OKS_BILL_CONT_LINES;
CREATE TABLE bronze_bec_ods_stg.OKS_BILL_CONT_LINES AS
SELECT
  *
FROM bec_raw_dl_ext.OKS_BILL_CONT_LINES
WHERE
  kca_operation <> 'DELETE'
  AND (ID, last_update_date) IN (
    SELECT
      ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.OKS_BILL_CONT_LINES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      ID
  );