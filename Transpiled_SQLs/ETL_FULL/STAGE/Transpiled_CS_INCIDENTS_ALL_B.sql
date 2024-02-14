DROP TABLE IF EXISTS bronze_bec_ods_stg.CS_INCIDENTS_ALL_B;
CREATE TABLE bronze_bec_ods_stg.CS_INCIDENTS_ALL_B AS
SELECT
  *
FROM bec_raw_dl_ext.CS_INCIDENTS_ALL_B
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(INCIDENT_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(INCIDENT_ID, 0) AS INCIDENT_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.CS_INCIDENTS_ALL_B
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(INCIDENT_ID, 0)
  );