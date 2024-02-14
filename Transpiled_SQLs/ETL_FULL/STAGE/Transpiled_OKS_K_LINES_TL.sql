DROP TABLE IF EXISTS bronze_bec_ods_stg.OKS_K_LINES_TL;
CREATE TABLE bronze_bec_ods_stg.OKS_K_LINES_TL AS
SELECT
  *
FROM bec_raw_dl_ext.OKS_K_LINES_TL
WHERE
  kca_operation <> 'DELETE'
  AND (ID, LANGUAGE, last_update_date) IN (
    SELECT
      ID,
      LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.OKS_K_LINES_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      ID,
      LANGUAGE
  );