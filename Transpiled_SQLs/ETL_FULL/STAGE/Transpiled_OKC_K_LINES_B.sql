DROP TABLE IF EXISTS bronze_bec_ods_stg.OKC_K_LINES_B;
CREATE TABLE bronze_bec_ods_stg.OKC_K_LINES_B AS
SELECT
  *
FROM bec_raw_dl_ext.OKC_K_LINES_B
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(ID, 'NA'), COALESCE(CHR_ID, 'NA'), COALESCE(CLE_ID, 'NA'), COALESCE(LINE_NUMBER, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(ID, 'NA') AS ID,
      COALESCE(CHR_ID, 'NA') AS CHR_ID,
      COALESCE(CLE_ID, 'NA') AS CLE_ID,
      COALESCE(LINE_NUMBER, 'NA') AS LINE_NUMBER,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.OKC_K_LINES_B
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(ID, 'NA'),
      COALESCE(CHR_ID, 'NA'),
      COALESCE(CLE_ID, 'NA'),
      COALESCE(LINE_NUMBER, 'NA')
  );