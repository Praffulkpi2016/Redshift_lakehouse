DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_LEGAL_ENTITIES_BSVS;
CREATE TABLE bronze_bec_ods_stg.GL_LEGAL_ENTITIES_BSVS AS
SELECT
  *
FROM bec_raw_dl_ext.GL_LEGAL_ENTITIES_BSVS
WHERE
  kca_operation <> 'DELETE'
  AND (LEGAL_ENTITY_ID, FLEX_VALUE_SET_ID, FLEX_SEGMENT_VALUE, last_update_date) IN (
    SELECT
      LEGAL_ENTITY_ID,
      FLEX_VALUE_SET_ID,
      FLEX_SEGMENT_VALUE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_LEGAL_ENTITIES_BSVS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      LEGAL_ENTITY_ID,
      FLEX_VALUE_SET_ID,
      FLEX_SEGMENT_VALUE
  );