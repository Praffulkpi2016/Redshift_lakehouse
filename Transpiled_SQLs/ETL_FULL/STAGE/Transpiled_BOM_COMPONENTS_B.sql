DROP TABLE IF EXISTS bronze_bec_ods_stg.BOM_COMPONENTS_B;
CREATE TABLE bronze_bec_ods_stg.BOM_COMPONENTS_B AS
SELECT
  *
FROM bec_raw_dl_ext.BOM_COMPONENTS_B
WHERE
  kca_operation <> 'DELETE'
  AND (COMPONENT_SEQUENCE_ID, last_update_date) IN (
    SELECT
      COMPONENT_SEQUENCE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.BOM_COMPONENTS_B
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COMPONENT_SEQUENCE_ID
  );