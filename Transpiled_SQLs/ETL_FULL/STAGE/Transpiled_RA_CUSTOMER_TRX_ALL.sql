DROP TABLE IF EXISTS bronze_bec_ods_stg.RA_CUSTOMER_TRX_ALL;
CREATE TABLE bronze_bec_ods_stg.RA_CUSTOMER_TRX_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.RA_CUSTOMER_TRX_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (CUSTOMER_TRX_ID, last_update_date) IN (
    SELECT
      CUSTOMER_TRX_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.RA_CUSTOMER_TRX_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      CUSTOMER_TRX_ID
  );