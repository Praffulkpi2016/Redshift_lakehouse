DROP TABLE IF EXISTS bronze_bec_ods_stg.PO_UN_NUMBERS_TL;
CREATE TABLE bronze_bec_ods_stg.PO_UN_NUMBERS_TL AS
SELECT
  *
FROM bec_raw_dl_ext.PO_UN_NUMBERS_TL
WHERE
  kca_operation <> 'DELETE'
  AND (UN_NUMBER_ID, LANGUAGE, last_update_date) IN (
    SELECT
      UN_NUMBER_ID,
      LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.PO_UN_NUMBERS_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      UN_NUMBER_ID,
      LANGUAGE
  );