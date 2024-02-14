DROP TABLE IF EXISTS bronze_bec_ods_stg.csi_item_instances;
CREATE TABLE bronze_bec_ods_stg.csi_item_instances AS
SELECT
  *
FROM bec_raw_dl_ext.csi_item_instances
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(INSTANCE_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(INSTANCE_ID, 0) AS INSTANCE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.csi_item_instances
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(INSTANCE_ID, 0)
  );