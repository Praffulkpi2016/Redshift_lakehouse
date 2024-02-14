DROP TABLE IF EXISTS bronze_bec_ods_stg.OKS_K_LINES_B;
CREATE TABLE bronze_bec_ods_stg.OKS_K_LINES_B AS
SELECT
  *
FROM bec_raw_dl_ext.OKS_K_LINES_B
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(ID, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(ID, 'NA') AS ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.OKS_K_LINES_B
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(ID, 'NA')
  );