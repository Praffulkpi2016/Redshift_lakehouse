DROP TABLE IF EXISTS bronze_bec_ods_stg.ALR_OUTPUT_HISTORY;
CREATE TABLE bronze_bec_ods_stg.alr_output_history AS
SELECT
  *
FROM bec_raw_dl_ext.alr_output_history
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(APPLICATION_ID, 0), COALESCE(CHECK_ID, 0), COALESCE(ROW_NUMBER, 0), COALESCE(NAME, 'NA'), kca_seq_date) IN (
    SELECT
      COALESCE(APPLICATION_ID, 0) AS APPLICATION_ID,
      COALESCE(CHECK_ID, 0) AS CHECK_ID,
      COALESCE(ROW_NUMBER, 0) AS ROW_NUMBER,
      COALESCE(NAME, 'NA') AS NAME,
      MAX(kca_seq_date) AS kca_seq_date
    FROM bec_raw_dl_ext.alr_output_history
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(APPLICATION_ID, 0),
      COALESCE(CHECK_ID, 0),
      COALESCE(ROW_NUMBER, 0),
      COALESCE(NAME, 'NA')
  );