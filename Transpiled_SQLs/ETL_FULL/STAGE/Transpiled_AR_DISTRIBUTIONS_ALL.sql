DROP TABLE IF EXISTS bronze_bec_ods_stg.AR_DISTRIBUTIONS_ALL;
CREATE TABLE bronze_bec_ods_stg.AR_DISTRIBUTIONS_ALL AS
SELECT
  *
FROM bec_raw_dl_ext.AR_DISTRIBUTIONS_ALL
WHERE
  kca_operation <> 'DELETE'
  AND (SOURCE_ID, SOURCE_TABLE, SOURCE_TYPE, LINE_ID, last_update_date) IN (
    SELECT
      SOURCE_ID,
      SOURCE_TABLE,
      SOURCE_TYPE,
      LINE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.AR_DISTRIBUTIONS_ALL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      SOURCE_ID,
      SOURCE_TABLE,
      SOURCE_TYPE,
      LINE_ID
  );