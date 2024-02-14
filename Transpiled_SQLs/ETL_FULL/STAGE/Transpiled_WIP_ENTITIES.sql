DROP TABLE IF EXISTS bronze_bec_ods_stg.WIP_ENTITIES;
CREATE TABLE bronze_bec_ods_stg.WIP_ENTITIES AS
SELECT
  *
FROM bec_raw_dl_ext.WIP_ENTITIES
WHERE
  kca_operation <> 'DELETE'
  AND (WIP_ENTITY_ID, last_update_date) IN (
    SELECT
      WIP_ENTITY_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.WIP_ENTITIES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      WIP_ENTITY_ID
  );