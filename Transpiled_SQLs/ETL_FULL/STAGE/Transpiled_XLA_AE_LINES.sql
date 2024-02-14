DROP TABLE IF EXISTS bronze_bec_ods_stg.XLA_AE_LINES;
CREATE TABLE bronze_bec_ods_stg.XLA_AE_LINES AS
SELECT
  *
FROM bec_raw_dl_ext.XLA_AE_LINES
WHERE
  kca_operation <> 'DELETE'
  AND (AE_HEADER_ID, AE_LINE_NUM, APPLICATION_ID, last_update_date) IN (
    SELECT
      AE_HEADER_ID,
      AE_LINE_NUM,
      APPLICATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.XLA_AE_LINES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      AE_HEADER_ID,
      AE_LINE_NUM,
      APPLICATION_ID
  );