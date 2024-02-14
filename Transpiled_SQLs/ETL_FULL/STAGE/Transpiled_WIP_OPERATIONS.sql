DROP table IF EXISTS bronze_bec_ods_stg.WIP_OPERATIONS;
CREATE TABLE bronze_bec_ods_stg.WIP_OPERATIONS AS
(
  SELECT
    *
  FROM bec_raw_dl_ext.WIP_OPERATIONS
  WHERE
    kca_operation <> 'DELETE'
    AND (COALESCE(WIP_ENTITY_ID, 0), COALESCE(OPERATION_SEQ_NUM, 0), COALESCE(REPETITIVE_SCHEDULE_ID, 0), last_update_date) IN (
      SELECT
        COALESCE(WIP_ENTITY_ID, 0) AS WIP_ENTITY_ID,
        COALESCE(OPERATION_SEQ_NUM, 0) AS OPERATION_SEQ_NUM,
        COALESCE(REPETITIVE_SCHEDULE_ID, 0) AS REPETITIVE_SCHEDULE_ID,
        MAX(last_update_date)
      FROM bec_raw_dl_ext.WIP_OPERATIONS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
      GROUP BY
        WIP_ENTITY_ID,
        OPERATION_SEQ_NUM,
        REPETITIVE_SCHEDULE_ID
    )
);