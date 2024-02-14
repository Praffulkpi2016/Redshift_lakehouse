DROP TABLE IF EXISTS bronze_bec_ods_stg.BOM_RESOURCES;
CREATE TABLE bronze_bec_ods_stg.BOM_RESOURCES AS
SELECT
  *
FROM bec_raw_dl_ext.BOM_RESOURCES
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(RESOURCE_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(RESOURCE_ID, 0),
      MAX(last_update_date)
    FROM bec_raw_dl_ext.BOM_RESOURCES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(RESOURCE_ID, 0)
  );