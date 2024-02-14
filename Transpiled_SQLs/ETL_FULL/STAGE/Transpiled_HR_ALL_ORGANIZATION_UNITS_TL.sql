DROP TABLE IF EXISTS bronze_bec_ods_stg.HR_ALL_ORGANIZATION_UNITS_TL;
CREATE TABLE bronze_bec_ods_stg.HR_ALL_ORGANIZATION_UNITS_TL AS
SELECT
  *
FROM bec_raw_dl_ext.HR_ALL_ORGANIZATION_UNITS_TL
WHERE
  kca_operation <> 'DELETE'
  AND (ORGANIZATION_ID, LANGUAGE, last_update_date) IN (
    SELECT
      ORGANIZATION_ID,
      LANGUAGE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.HR_ALL_ORGANIZATION_UNITS_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      ORGANIZATION_ID,
      LANGUAGE
  );