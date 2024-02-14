DROP TABLE IF EXISTS bronze_bec_ods_stg.HZ_RELATIONSHIPS;
CREATE TABLE bronze_bec_ods_stg.HZ_RELATIONSHIPS AS
SELECT
  *
FROM bec_raw_dl_ext.HZ_RELATIONSHIPS
WHERE
  kca_operation <> 'DELETE'
  AND (RELATIONSHIP_ID, DIRECTIONAL_FLAG, last_update_date) IN (
    SELECT
      RELATIONSHIP_ID,
      DIRECTIONAL_FLAG,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HZ_RELATIONSHIPS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      RELATIONSHIP_ID,
      DIRECTIONAL_FLAG
  );