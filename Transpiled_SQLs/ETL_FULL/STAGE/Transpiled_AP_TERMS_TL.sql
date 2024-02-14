DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_TERMS_TL;
CREATE TABLE bronze_bec_ods_stg.AP_TERMS_TL AS
SELECT
  *
FROM bec_raw_dl_ext.AP_TERMS_TL
WHERE
  kca_operation <> 'DELETE'
  AND (term_id, language, last_update_date) IN (
    SELECT
      term_id,
      language,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_TERMS_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      term_id,
      language
  );