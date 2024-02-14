DROP TABLE IF EXISTS bronze_bec_ods_stg.XLE_ENTITY_PROFILES;
CREATE TABLE bronze_bec_ods_stg.XLE_ENTITY_PROFILES AS
SELECT
  *
FROM bec_raw_dl_ext.XLE_ENTITY_PROFILES
WHERE
  kca_operation <> 'DELETE'
  AND (LEGAL_ENTITY_ID, last_update_date) IN (
    SELECT
      LEGAL_ENTITY_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.XLE_ENTITY_PROFILES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      LEGAL_ENTITY_ID
  );