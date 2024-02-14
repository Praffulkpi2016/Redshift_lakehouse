DROP TABLE IF EXISTS bronze_bec_ods_stg.FINANCIALS_SYSTEM_PARAMS_ALL;
CREATE TABLE bronze_bec_ods_stg.FINANCIALS_SYSTEM_PARAMS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.FINANCIALS_SYSTEM_PARAMS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (ORG_ID, SET_OF_BOOKS_ID, last_update_date) IN (
    SELECT
      ORG_ID,
      SET_OF_BOOKS_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.FINANCIALS_SYSTEM_PARAMS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      ORG_ID,
      SET_OF_BOOKS_ID
  );