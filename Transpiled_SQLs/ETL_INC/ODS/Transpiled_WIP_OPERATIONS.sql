/* Delete Records */
DELETE FROM silver_bec_ods.WIP_OPERATIONS
WHERE
  (COALESCE(WIP_ENTITY_ID, 0), COALESCE(OPERATION_SEQ_NUM, 0), COALESCE(REPETITIVE_SCHEDULE_ID, 0)) IN (
    SELECT
      COALESCE(stg.WIP_ENTITY_ID, 0) AS WIP_ENTITY_ID,
      COALESCE(stg.OPERATION_SEQ_NUM, 0) AS OPERATION_SEQ_NUM,
      COALESCE(stg.REPETITIVE_SCHEDULE_ID, 0) AS REPETITIVE_SCHEDULE_ID
    FROM silver_bec_ods.WIP_OPERATIONS AS ods, bronze_bec_ods_stg.WIP_OPERATIONS AS stg
    WHERE
      COALESCE(ods.WIP_ENTITY_ID, 0) = COALESCE(stg.WIP_ENTITY_ID, 0)
      AND COALESCE(ods.OPERATION_SEQ_NUM, 0) = COALESCE(stg.OPERATION_SEQ_NUM, 0)
      AND COALESCE(ods.REPETITIVE_SCHEDULE_ID, 0) = COALESCE(stg.REPETITIVE_SCHEDULE_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.WIP_OPERATIONS (
  wip_entity_id,
  operation_seq_num,
  organization_id,
  repetitive_schedule_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  operation_sequence_id,
  standard_operation_id,
  department_id,
  description,
  scheduled_quantity,
  quantity_in_queue,
  quantity_running,
  quantity_waiting_to_move,
  quantity_rejected,
  quantity_scrapped,
  quantity_completed,
  first_unit_start_date,
  first_unit_completion_date,
  last_unit_start_date,
  last_unit_completion_date,
  previous_operation_seq_num,
  next_operation_seq_num,
  count_point_type,
  backflush_flag,
  minimum_transfer_quantity,
  date_last_moved,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  wf_itemtype,
  wf_itemkey,
  operation_yield,
  operation_yield_enabled,
  pre_split_quantity,
  operation_completed,
  shutdown_type,
  x_pos,
  y_pos,
  previous_operation_seq_id,
  skip_flag,
  long_description,
  cumulative_scrap_quantity,
  disable_date,
  recommended,
  progress_percentage,
  wsm_op_seq_num,
  wsm_bonus_quantity,
  employee_id,
  actual_start_date,
  actual_completion_date,
  projected_completion_date,
  wsm_update_quantity_txn_id,
  wsm_costed_quantity_completed,
  lowest_acceptable_yield,
  check_skill,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    wip_entity_id,
    operation_seq_num,
    organization_id,
    repetitive_schedule_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    operation_sequence_id,
    standard_operation_id,
    department_id,
    description,
    scheduled_quantity,
    quantity_in_queue,
    quantity_running,
    quantity_waiting_to_move,
    quantity_rejected,
    quantity_scrapped,
    quantity_completed,
    first_unit_start_date,
    first_unit_completion_date,
    last_unit_start_date,
    last_unit_completion_date,
    previous_operation_seq_num,
    next_operation_seq_num,
    count_point_type,
    backflush_flag,
    minimum_transfer_quantity,
    date_last_moved,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    wf_itemtype,
    wf_itemkey,
    operation_yield,
    operation_yield_enabled,
    pre_split_quantity,
    operation_completed,
    shutdown_type,
    x_pos,
    y_pos,
    previous_operation_seq_id,
    skip_flag,
    long_description,
    cumulative_scrap_quantity,
    disable_date,
    recommended,
    progress_percentage,
    wsm_op_seq_num,
    wsm_bonus_quantity,
    employee_id,
    actual_start_date,
    actual_completion_date,
    projected_completion_date,
    wsm_update_quantity_txn_id,
    wsm_costed_quantity_completed,
    lowest_acceptable_yield,
    check_skill,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.WIP_OPERATIONS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(WIP_ENTITY_ID, 0), COALESCE(OPERATION_SEQ_NUM, 0), COALESCE(REPETITIVE_SCHEDULE_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(WIP_ENTITY_ID, 0) AS WIP_ENTITY_ID,
        COALESCE(OPERATION_SEQ_NUM, 0) AS OPERATION_SEQ_NUM,
        COALESCE(REPETITIVE_SCHEDULE_ID, 0) AS REPETITIVE_SCHEDULE_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.WIP_OPERATIONS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        WIP_ENTITY_ID,
        OPERATION_SEQ_NUM,
        REPETITIVE_SCHEDULE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.WIP_OPERATIONS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.WIP_OPERATIONS SET IS_DELETED_FLG = 'Y'
WHERE
  (COALESCE(WIP_ENTITY_ID, 0), COALESCE(OPERATION_SEQ_NUM, 0), COALESCE(REPETITIVE_SCHEDULE_ID, 0)) IN (
    SELECT
      COALESCE(WIP_ENTITY_ID, 0),
      COALESCE(OPERATION_SEQ_NUM, 0),
      COALESCE(REPETITIVE_SCHEDULE_ID, 0)
    FROM bec_raw_dl_ext.WIP_OPERATIONS
    WHERE
      (COALESCE(WIP_ENTITY_ID, 0), COALESCE(OPERATION_SEQ_NUM, 0), COALESCE(REPETITIVE_SCHEDULE_ID, 0), KCA_SEQ_ID) IN (
        SELECT
          COALESCE(WIP_ENTITY_ID, 0),
          COALESCE(OPERATION_SEQ_NUM, 0),
          COALESCE(REPETITIVE_SCHEDULE_ID, 0),
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.WIP_OPERATIONS
        GROUP BY
          COALESCE(WIP_ENTITY_ID, 0),
          COALESCE(OPERATION_SEQ_NUM, 0),
          COALESCE(REPETITIVE_SCHEDULE_ID, 0)
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'wip_operations';