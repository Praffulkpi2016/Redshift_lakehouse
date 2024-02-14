DROP TABLE IF EXISTS bronze_bec_ods_stg.AP_TERMS_LINES;
CREATE TABLE bronze_bec_ods_stg.AP_TERMS_LINES AS
SELECT
  *
FROM bec_raw_dl_ext.AP_TERMS_LINES
WHERE
  kca_operation <> 'DELETE'
  AND (term_id, SEQUENCE_NUM, last_update_date) IN (
    SELECT
      term_id,
      SEQUENCE_NUM,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AP_TERMS_LINES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      term_id,
      SEQUENCE_NUM
  );