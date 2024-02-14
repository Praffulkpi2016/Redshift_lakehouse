DROP TABLE IF EXISTS bronze_bec_ods_stg.IBY_PMT_INSTR_USES_ALL;
CREATE TABLE bronze_bec_ods_stg.IBY_PMT_INSTR_USES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.IBY_PMT_INSTR_USES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (INSTRUMENT_PAYMENT_USE_ID, last_update_date) IN (
    SELECT
      INSTRUMENT_PAYMENT_USE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.IBY_PMT_INSTR_USES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      INSTRUMENT_PAYMENT_USE_ID
  );