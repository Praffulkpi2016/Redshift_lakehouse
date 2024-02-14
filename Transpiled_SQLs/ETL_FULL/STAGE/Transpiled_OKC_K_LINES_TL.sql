DROP TABLE IF EXISTS bronze_bec_ods_stg.OKC_K_LINES_TL;
CREATE TABLE bronze_bec_ods_stg.OKC_K_LINES_TL AS
SELECT
  *
FROM bec_raw_dl_ext.OKC_K_LINES_TL
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(ID, 'NA'), COALESCE(LANGUAGE, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(ID, 'NA'),
      COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.OKC_K_LINES_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(ID, 'NA'),
      COALESCE(LANGUAGE, 'NA')
  );