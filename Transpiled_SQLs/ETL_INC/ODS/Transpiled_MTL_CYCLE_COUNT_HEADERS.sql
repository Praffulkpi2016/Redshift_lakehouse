/* Delete Records */
DELETE FROM silver_bec_ods.MTL_CYCLE_COUNT_HEADERS
WHERE
  (
    COALESCE(CYCLE_COUNT_HEADER_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.CYCLE_COUNT_HEADER_ID, 0) AS CYCLE_COUNT_HEADER_ID
    FROM silver_bec_ods.MTL_CYCLE_COUNT_HEADERS AS ods, bronze_bec_ods_stg.MTL_CYCLE_COUNT_HEADERS AS stg
    WHERE
      COALESCE(ods.CYCLE_COUNT_HEADER_ID, 0) = COALESCE(stg.CYCLE_COUNT_HEADER_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_CYCLE_COUNT_HEADERS (
  cycle_count_header_id,
  organization_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  cycle_count_header_name,
  inventory_adjustment_account,
  orientation_code,
  abc_assignment_group_id,
  onhand_visible_flag,
  days_until_late,
  autoschedule_enabled_flag,
  schedule_interval_time,
  zero_count_flag,
  header_last_schedule_date,
  header_next_schedule_date,
  disable_date,
  approval_option_code,
  automatic_recount_flag,
  next_user_count_sequence,
  unscheduled_count_entry,
  cycle_count_calendar,
  calendar_exception_set,
  approval_tolerance_positive,
  approval_tolerance_negative,
  cost_tolerance_positive,
  cost_tolerance_negative,
  hit_miss_tolerance_positive,
  hit_miss_tolerance_negative,
  abc_initialization_status,
  description,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
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
  maximum_auto_recounts,
  serial_count_option,
  serial_detail_option,
  serial_adjustment_option,
  serial_discrepancy_option,
  container_adjustment_option,
  container_discrepancy_option,
  container_enabled_flag,
  cycle_count_type,
  schedule_empty_locations,
  default_num_counts_per_year,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    cycle_count_header_id,
    organization_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    cycle_count_header_name,
    inventory_adjustment_account,
    orientation_code,
    abc_assignment_group_id,
    onhand_visible_flag,
    days_until_late,
    autoschedule_enabled_flag,
    schedule_interval_time,
    zero_count_flag,
    header_last_schedule_date,
    header_next_schedule_date,
    disable_date,
    approval_option_code,
    automatic_recount_flag,
    next_user_count_sequence,
    unscheduled_count_entry,
    cycle_count_calendar,
    calendar_exception_set,
    approval_tolerance_positive,
    approval_tolerance_negative,
    cost_tolerance_positive,
    cost_tolerance_negative,
    hit_miss_tolerance_positive,
    hit_miss_tolerance_negative,
    abc_initialization_status,
    description,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
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
    maximum_auto_recounts,
    serial_count_option,
    serial_detail_option,
    serial_adjustment_option,
    serial_discrepancy_option,
    container_adjustment_option,
    container_discrepancy_option,
    container_enabled_flag,
    cycle_count_type,
    schedule_empty_locations,
    default_num_counts_per_year,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_CYCLE_COUNT_HEADERS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(CYCLE_COUNT_HEADER_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(CYCLE_COUNT_HEADER_ID, 0) AS CYCLE_COUNT_HEADER_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MTL_CYCLE_COUNT_HEADERS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(CYCLE_COUNT_HEADER_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_CYCLE_COUNT_HEADERS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_CYCLE_COUNT_HEADERS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    CYCLE_COUNT_HEADER_ID
  ) IN (
    SELECT
      CYCLE_COUNT_HEADER_ID
    FROM bec_raw_dl_ext.MTL_CYCLE_COUNT_HEADERS
    WHERE
      (CYCLE_COUNT_HEADER_ID, KCA_SEQ_ID) IN (
        SELECT
          CYCLE_COUNT_HEADER_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_CYCLE_COUNT_HEADERS
        GROUP BY
          CYCLE_COUNT_HEADER_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_cycle_count_headers';