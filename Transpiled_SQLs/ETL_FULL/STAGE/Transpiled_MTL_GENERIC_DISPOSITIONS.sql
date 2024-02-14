DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_GENERIC_DISPOSITIONS;
CREATE TABLE bronze_bec_ods_stg.MTL_GENERIC_DISPOSITIONS AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_GENERIC_DISPOSITIONS
WHERE
  kca_operation <> 'DELETE'
  AND (DISPOSITION_ID, ORGANIZATION_ID, last_update_date) IN (
    SELECT
      DISPOSITION_ID,
      ORGANIZATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MTL_GENERIC_DISPOSITIONS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      DISPOSITION_ID,
      ORGANIZATION_ID
  );