DROP TABLE IF EXISTS bronze_bec_ods_stg.CS_INCIDENT_TYPES_TL;
CREATE TABLE bronze_bec_ods_stg.CS_INCIDENT_TYPES_TL AS
SELECT
  *
FROM bec_raw_dl_ext.CS_INCIDENT_TYPES_TL
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(INCIDENT_TYPE_ID, 0), COALESCE(LANGUAGE, 'NA'), last_update_date) IN (
    SELECT
      COALESCE(INCIDENT_TYPE_ID, 0) AS INCIDENT_TYPE_ID,
      COALESCE(LANGUAGE, 'NA') AS LANGUAGE,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.CS_INCIDENT_TYPES_TL
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(INCIDENT_TYPE_ID, 0),
      COALESCE(LANGUAGE, 'NA')
  );