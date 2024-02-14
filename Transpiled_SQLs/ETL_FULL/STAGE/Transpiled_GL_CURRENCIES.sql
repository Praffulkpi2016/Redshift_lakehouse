DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_CURRENCIES;
CREATE TABLE bronze_bec_ods_stg.GL_CURRENCIES AS
SELECT
  *
FROM bec_raw_dl_ext.FND_CURRENCIES
WHERE
  kca_operation <> 'DELETE'
  AND (CURRENCY_CODE, last_update_date) IN (
    SELECT
      CURRENCY_CODE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.FND_CURRENCIES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      CURRENCY_CODE
  );