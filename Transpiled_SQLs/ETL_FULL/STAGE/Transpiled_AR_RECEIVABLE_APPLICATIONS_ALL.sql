DROP TABLE IF EXISTS bronze_bec_ods_stg.AR_RECEIVABLE_APPLICATIONS_ALL;
CREATE TABLE bronze_bec_ods_stg.AR_RECEIVABLE_APPLICATIONS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AR_RECEIVABLE_APPLICATIONS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (RECEIVABLE_APPLICATION_ID, last_update_date) IN (
    SELECT
      RECEIVABLE_APPLICATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AR_RECEIVABLE_APPLICATIONS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      RECEIVABLE_APPLICATION_ID
  );