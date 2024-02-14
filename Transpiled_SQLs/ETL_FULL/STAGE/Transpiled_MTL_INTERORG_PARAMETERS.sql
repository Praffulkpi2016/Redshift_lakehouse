DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_INTERORG_PARAMETERS;
CREATE TABLE bronze_bec_ods_stg.MTL_INTERORG_PARAMETERS AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_INTERORG_PARAMETERS
WHERE
  kca_operation <> 'DELETE'
  AND (FROM_ORGANIZATION_ID, TO_ORGANIZATION_ID, last_update_date) IN (
    SELECT
      FROM_ORGANIZATION_ID,
      TO_ORGANIZATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_INTERORG_PARAMETERS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      FROM_ORGANIZATION_ID,
      TO_ORGANIZATION_ID
  );