DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_LE_VALUE_SETS;
CREATE TABLE bronze_bec_ods_stg.GL_LE_VALUE_SETS AS
SELECT
  *
FROM bec_raw_dl_ext.GL_LE_VALUE_SETS
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(LEGAL_ENTITY_ID, 0), COALESCE(FLEX_VALUE_SET_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(LEGAL_ENTITY_ID, 0),
      COALESCE(FLEX_VALUE_SET_ID, 0),
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_LE_VALUE_SETS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(LEGAL_ENTITY_ID, 0),
      COALESCE(FLEX_VALUE_SET_ID, 0)
  );