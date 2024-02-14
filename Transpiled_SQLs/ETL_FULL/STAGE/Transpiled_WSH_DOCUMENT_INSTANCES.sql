DROP TABLE IF EXISTS bronze_bec_ods_stg.WSH_DOCUMENT_INSTANCES;
CREATE TABLE bronze_bec_ods_stg.WSH_DOCUMENT_INSTANCES AS
SELECT
  *
FROM bec_raw_dl_ext.WSH_DOCUMENT_INSTANCES
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(DOCUMENT_INSTANCE_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(DOCUMENT_INSTANCE_ID, 0) AS DOCUMENT_INSTANCE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.WSH_DOCUMENT_INSTANCES
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(DOCUMENT_INSTANCE_ID, 0)
  );