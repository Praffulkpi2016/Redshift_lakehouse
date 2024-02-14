DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_CODE_COMBINATIONS;
CREATE TABLE bronze_bec_ods_stg.GL_CODE_COMBINATIONS AS
SELECT
  *
FROM bec_raw_dl_ext.GL_CODE_COMBINATIONS
WHERE
  kca_operation <> 'DELETE'
  AND (code_combination_id, last_update_date) IN (
    SELECT
      code_combination_id,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_CODE_COMBINATIONS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      code_combination_id
  );