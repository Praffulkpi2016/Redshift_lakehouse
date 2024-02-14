DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_DAILY_RATES;
CREATE TABLE bronze_bec_ods_stg.GL_DAILY_RATES AS
SELECT
  *
FROM bec_raw_dl_ext.GL_DAILY_RATES
WHERE
  kca_operation <> 'DELETE'
  AND (FROM_CURRENCY, TO_CURRENCY, CONVERSION_DATE, CONVERSION_TYPE, last_update_date) IN (
    SELECT
      FROM_CURRENCY,
      TO_CURRENCY,
      CONVERSION_DATE,
      CONVERSION_TYPE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_DAILY_RATES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      FROM_CURRENCY,
      TO_CURRENCY,
      CONVERSION_DATE,
      CONVERSION_TYPE
  );