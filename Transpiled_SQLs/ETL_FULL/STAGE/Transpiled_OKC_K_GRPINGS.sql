DROP TABLE IF EXISTS bronze_bec_ods_stg.OKC_K_GRPINGS;
CREATE TABLE bronze_bec_ods_stg.OKC_K_GRPINGS AS
SELECT
  *
FROM bec_raw_dl_ext.OKC_K_GRPINGS
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(ID, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(ID, 'NA') AS ID,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.OKC_K_GRPINGS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(ID, 'NA')
  );