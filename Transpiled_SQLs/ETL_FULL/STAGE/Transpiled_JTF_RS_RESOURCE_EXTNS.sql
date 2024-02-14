DROP TABLE IF EXISTS bronze_bec_ods_stg.JTF_RS_RESOURCE_EXTNS;
CREATE TABLE bronze_bec_ods_stg.JTF_RS_RESOURCE_EXTNS AS
SELECT
  *
FROM bec_raw_dl_ext.JTF_RS_RESOURCE_EXTNS
WHERE
  kca_operation <> 'DELETE'
  AND (RESOURCE_ID, last_update_date) IN (
    SELECT
      RESOURCE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.JTF_RS_RESOURCE_EXTNS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      RESOURCE_ID
  );