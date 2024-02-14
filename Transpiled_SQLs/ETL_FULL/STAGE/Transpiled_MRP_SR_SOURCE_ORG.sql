DROP TABLE IF EXISTS bronze_bec_ods_stg.MRP_SR_SOURCE_ORG;
CREATE TABLE bronze_bec_ods_stg.MRP_SR_SOURCE_ORG AS
SELECT
  *
FROM bec_raw_dl_ext.MRP_SR_SOURCE_ORG
WHERE
  kca_operation <> 'DELETE'
  AND (SR_SOURCE_ID, last_update_date) IN (
    SELECT
      SR_SOURCE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.MRP_SR_SOURCE_ORG
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      SR_SOURCE_ID
  );