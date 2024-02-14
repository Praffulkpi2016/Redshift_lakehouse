DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_JE_LINES;
CREATE TABLE bronze_bec_ods_stg.GL_JE_LINES AS
SELECT
  *
FROM bec_raw_dl_ext.GL_JE_LINES
WHERE
  kca_operation <> 'DELETE'
  AND (je_header_id, JE_LINE_NUM, last_update_date) IN (
    SELECT
      je_header_id,
      JE_LINE_NUM,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_JE_LINES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      je_header_id,
      JE_LINE_NUM
  );