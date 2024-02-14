DROP TABLE IF EXISTS bronze_bec_ods_stg.HR_ORGANIZATION_INFORMATION;
CREATE TABLE bronze_bec_ods_stg.HR_ORGANIZATION_INFORMATION AS
SELECT
  *
FROM bec_raw_dl_ext.HR_ORGANIZATION_INFORMATION
WHERE
  kca_operation <> 'DELETE'
  AND (ORG_INFORMATION_ID, last_update_date) IN (
    SELECT
      ORG_INFORMATION_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HR_ORGANIZATION_INFORMATION
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      ORG_INFORMATION_ID
  );