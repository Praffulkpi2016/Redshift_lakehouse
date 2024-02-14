DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_ENCUMBRANCE_TYPES;
CREATE TABLE bronze_bec_ods_stg.GL_ENCUMBRANCE_TYPES AS
SELECT
  *
FROM bec_raw_dl_ext.GL_ENCUMBRANCE_TYPES
WHERE
  kca_operation <> 'DELETE'
  AND (ENCUMBRANCE_TYPE_ID, last_update_date) IN (
    SELECT
      ENCUMBRANCE_TYPE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_ENCUMBRANCE_TYPES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      ENCUMBRANCE_TYPE_ID
  );