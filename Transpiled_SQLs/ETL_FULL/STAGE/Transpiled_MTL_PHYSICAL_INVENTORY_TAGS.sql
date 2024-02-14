DROP TABLE IF EXISTS bronze_bec_ods_stg.MTL_PHYSICAL_INVENTORY_TAGS;
CREATE TABLE bronze_bec_ods_stg.MTL_PHYSICAL_INVENTORY_TAGS AS
SELECT
  *
FROM bec_raw_dl_ext.MTL_PHYSICAL_INVENTORY_TAGS
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(TAG_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(TAG_ID, 0) AS TAG_ID,
      MAX(last_update_date) AS last_update_date
    FROM bec_raw_dl_ext.MTL_PHYSICAL_INVENTORY_TAGS
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(TAG_ID, 0)
  );