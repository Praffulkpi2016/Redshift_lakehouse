DROP TABLE IF EXISTS bronze_bec_ods_stg.RA_TERMS_TL;
CREATE TABLE bronze_bec_ods_stg.RA_TERMS_TL AS
SELECT
  *
FROM bec_raw_dl_ext.RA_TERMS_TL
WHERE
  kca_operation <> 'DELETE'
  AND (TERM_ID, LANGUAGE, last_update_date) IN (
    SELECT
      TERM_ID,
      LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.RA_TERMS_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      TERM_ID,
      LANGUAGE
  );