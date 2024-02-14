DROP TABLE IF EXISTS bronze_bec_ods_stg.HR_ALL_ORGANIZATION_UNITS;
CREATE TABLE bronze_bec_ods_stg.HR_ALL_ORGANIZATION_UNITS AS
SELECT
  *
FROM bec_raw_dl_ext.HR_ALL_ORGANIZATION_UNITS
WHERE
  kca_operation <> 'DELETE'
  AND (ORGANIZATION_ID, last_update_date) IN (
    SELECT
      ORGANIZATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HR_ALL_ORGANIZATION_UNITS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      ORGANIZATION_ID
  );