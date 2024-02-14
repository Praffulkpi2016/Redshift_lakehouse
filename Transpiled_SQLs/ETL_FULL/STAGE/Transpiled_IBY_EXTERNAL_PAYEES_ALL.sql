DROP TABLE IF EXISTS bronze_bec_ods_stg.IBY_EXTERNAL_PAYEES_ALL;
CREATE TABLE bronze_bec_ods_stg.IBY_EXTERNAL_PAYEES_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.IBY_EXTERNAL_PAYEES_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(EXT_PAYEE_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(EXT_PAYEE_ID, 0) AS EXT_PAYEE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.IBY_EXTERNAL_PAYEES_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(EXT_PAYEE_ID, 0)
  );