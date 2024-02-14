DROP TABLE IF EXISTS bronze_bec_ods_stg.AR_RECEIVABLES_TRX_ALL;
CREATE TABLE bronze_bec_ods_stg.AR_RECEIVABLES_TRX_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AR_RECEIVABLES_TRX_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (RECEIVABLES_TRX_ID, COALESCE(org_id, 0), last_update_date) IN (
    SELECT
      RECEIVABLES_TRX_ID,
      COALESCE(org_id, 0),
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AR_RECEIVABLES_TRX_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      RECEIVABLES_TRX_ID,
      COALESCE(org_id, 0)
  );