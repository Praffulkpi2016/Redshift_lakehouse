DROP TABLE IF EXISTS bronze_bec_ods_stg.FND_ID_FLEX_SEGMENTS;
CREATE TABLE bronze_bec_ods_stg.FND_ID_FLEX_SEGMENTS AS
SELECT
  *
FROM bec_raw_dl_ext.FND_ID_FLEX_SEGMENTS
WHERE
  kca_operation <> 'DELETE'
  AND (APPLICATION_ID, ID_FLEX_CODE, ID_FLEX_NUM, APPLICATION_COLUMN_NAME, last_update_date) IN (
    SELECT
      APPLICATION_ID,
      ID_FLEX_CODE,
      ID_FLEX_NUM,
      APPLICATION_COLUMN_NAME,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.FND_ID_FLEX_SEGMENTS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      APPLICATION_ID,
      ID_FLEX_CODE,
      ID_FLEX_NUM,
      APPLICATION_COLUMN_NAME
  );