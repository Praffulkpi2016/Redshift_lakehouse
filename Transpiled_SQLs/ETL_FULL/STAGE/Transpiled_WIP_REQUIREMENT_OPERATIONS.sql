DROP TABLE IF EXISTS bronze_bec_ods_stg.wip_requirement_operations;
CREATE TABLE bronze_bec_ods_stg.wip_requirement_operations AS
SELECT
  *
FROM bec_raw_dl_ext.wip_requirement_operations
WHERE
  kca_operation <> 'DELETE'
  AND (COALESCE(INVENTORY_ITEM_ID, 0), COALESCE(WIP_ENTITY_ID, 0), COALESCE(OPERATION_SEQ_NUM, 0), COALESCE(REPETITIVE_SCHEDULE_ID, 0), last_update_date) IN (
    SELECT
      COALESCE(INVENTORY_ITEM_ID, 0) AS INVENTORY_ITEM_ID,
      COALESCE(WIP_ENTITY_ID, 0) AS WIP_ENTITY_ID,
      COALESCE(OPERATION_SEQ_NUM, 0) AS OPERATION_SEQ_NUM,
      COALESCE(REPETITIVE_SCHEDULE_ID, 0) AS REPETITIVE_SCHEDULE_ID,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.wip_requirement_operations
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      COALESCE(INVENTORY_ITEM_ID, 0),
      COALESCE(WIP_ENTITY_ID, 0),
      COALESCE(OPERATION_SEQ_NUM, 0),
      COALESCE(REPETITIVE_SCHEDULE_ID, 0)
  );