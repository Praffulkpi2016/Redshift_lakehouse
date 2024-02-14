DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_LEDGER_RELATIONSHIPS;
CREATE TABLE bronze_bec_ods_stg.GL_LEDGER_RELATIONSHIPS AS
SELECT
  *
FROM bec_raw_dl_ext.GL_LEDGER_RELATIONSHIPS
WHERE
  kca_operation <> 'DELETE'
  AND (RELATIONSHIP_ID, last_update_date) IN (
    SELECT
      RELATIONSHIP_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_LEDGER_RELATIONSHIPS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      RELATIONSHIP_ID
  );