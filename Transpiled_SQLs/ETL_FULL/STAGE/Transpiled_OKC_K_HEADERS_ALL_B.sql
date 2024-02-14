DROP TABLE IF EXISTS bronze_bec_ods_stg.OKC_K_HEADERS_ALL_B;
CREATE TABLE bronze_bec_ods_stg.OKC_K_HEADERS_ALL_B AS
SELECT
  *
FROM bec_raw_dl_ext.OKC_K_HEADERS_ALL_B
WHERE
  kca_operation <> 'DELETE'
  AND (ID, last_update_date) IN (
    SELECT
      ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.OKC_K_HEADERS_ALL_B
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      ID
  );