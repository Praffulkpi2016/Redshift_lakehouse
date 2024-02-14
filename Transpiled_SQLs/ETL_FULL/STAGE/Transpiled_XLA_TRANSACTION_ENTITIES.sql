DROP table IF EXISTS bronze_bec_ods_stg.XLA_TRANSACTION_ENTITIES;
CREATE TABLE bronze_bec_ods_stg.XLA_TRANSACTION_ENTITIES AS
SELECT
  *
FROM bec_raw_dl_ext.XLA_TRANSACTION_ENTITIES
WHERE
  kca_operation <> 'DELETE'
  AND (ENTITY_ID, last_update_date) IN (
    SELECT
      ENTITY_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.XLA_TRANSACTION_ENTITIES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      ENTITY_ID
  );