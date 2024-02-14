DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_PERIODS;
CREATE TABLE bronze_bec_ods_stg.GL_PERIODS AS
SELECT
  *
FROM bec_raw_dl_ext.GL_PERIODS
WHERE
  kca_operation <> 'DELETE'
  AND (PERIOD_SET_NAME, PERIOD_NAME, last_update_date) IN (
    SELECT
      PERIOD_SET_NAME,
      PERIOD_NAME,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_PERIODS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      PERIOD_SET_NAME,
      PERIOD_NAME
  );