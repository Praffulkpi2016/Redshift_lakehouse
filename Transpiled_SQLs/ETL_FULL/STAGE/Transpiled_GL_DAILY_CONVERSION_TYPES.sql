DROP table IF EXISTS bronze_bec_ods_stg.GL_DAILY_CONVERSION_TYPES;
CREATE TABLE bronze_bec_ods_stg.GL_DAILY_CONVERSION_TYPES AS
SELECT
  *
FROM bec_raw_dl_ext.GL_DAILY_CONVERSION_TYPES
WHERE
  kca_operation <> 'DELETE'
  AND (conversion_type, last_update_date) IN (
    SELECT
      conversion_type,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_DAILY_CONVERSION_TYPES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      conversion_type
  );