DROP TABLE IF EXISTS bronze_bec_ods_stg.FND_FLEX_VALUE_SETS;
CREATE TABLE bronze_bec_ods_stg.FND_FLEX_VALUE_SETS AS
SELECT
  *
FROM bec_raw_dl_ext.FND_FLEX_VALUE_SETS
WHERE
  kca_operation <> 'DELETE'
  AND (FLEX_VALUE_SET_ID, last_update_date) IN (
    SELECT
      FLEX_VALUE_SET_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.FND_FLEX_VALUE_SETS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      FLEX_VALUE_SET_ID
  );