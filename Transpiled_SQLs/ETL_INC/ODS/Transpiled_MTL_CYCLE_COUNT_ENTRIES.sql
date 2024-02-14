/* Delete Records */
DELETE FROM silver_bec_ods.mtl_cycle_count_entries
WHERE
  CYCLE_COUNT_ENTRY_ID IN (
    SELECT
      stg.CYCLE_COUNT_ENTRY_ID
    FROM silver_bec_ods.mtl_cycle_count_entries AS ods, bronze_bec_ods_stg.mtl_cycle_count_entries AS stg
    WHERE
      ods.CYCLE_COUNT_ENTRY_ID = stg.CYCLE_COUNT_ENTRY_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.mtl_cycle_count_entries (
  cycle_count_entry_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  count_list_sequence,
  count_date_first,
  count_date_current,
  count_date_prior,
  count_date_dummy,
  counted_by_employee_id_first,
  counted_by_employee_id_current,
  counted_by_employee_id_prior,
  counted_by_employee_id_dummy,
  count_uom_first,
  count_uom_current,
  count_uom_prior,
  count_quantity_first,
  count_quantity_current,
  count_quantity_prior,
  inventory_item_id,
  subinventory,
  entry_status_code,
  count_due_date,
  organization_id,
  cycle_count_header_id,
  number_of_counts,
  locator_id,
  adjustment_quantity,
  adjustment_date,
  adjustment_amount,
  item_unit_cost,
  inventory_adjustment_account,
  approval_date,
  approver_employee_id,
  revision,
  lot_number,
  lot_control,
  system_quantity_first,
  system_quantity_current,
  system_quantity_prior,
  reference_first,
  reference_current,
  reference_prior,
  primary_uom_quantity_first,
  primary_uom_quantity_current,
  primary_uom_quantity_prior,
  count_type_code,
  transaction_reason_id,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  approval_type,
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
  serial_number,
  serial_detail,
  approval_condition,
  neg_adjustment_quantity,
  neg_adjustment_amount,
  export_flag,
  parent_lpn_id,
  outermost_lpn_id,
  standard_operation_id,
  task_priority,
  cost_group_id,
  secondary_uom_quantity_first,
  secondary_uom_quantity_current,
  secondary_uom_quantity_prior,
  count_secondary_uom_first,
  count_secondary_uom_current,
  count_secondary_uom_prior,
  secondary_adjustment_quantity,
  secondary_system_qty_current,
  secondary_system_qty_first,
  secondary_system_qty_prior,
  lot_expiration_date,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    cycle_count_entry_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    count_list_sequence,
    count_date_first,
    count_date_current,
    count_date_prior,
    count_date_dummy,
    counted_by_employee_id_first,
    counted_by_employee_id_current,
    counted_by_employee_id_prior,
    counted_by_employee_id_dummy,
    count_uom_first,
    count_uom_current,
    count_uom_prior,
    count_quantity_first,
    count_quantity_current,
    count_quantity_prior,
    inventory_item_id,
    subinventory,
    entry_status_code,
    count_due_date,
    organization_id,
    cycle_count_header_id,
    number_of_counts,
    locator_id,
    adjustment_quantity,
    adjustment_date,
    adjustment_amount,
    item_unit_cost,
    inventory_adjustment_account,
    approval_date,
    approver_employee_id,
    revision,
    lot_number,
    lot_control,
    system_quantity_first,
    system_quantity_current,
    system_quantity_prior,
    reference_first,
    reference_current,
    reference_prior,
    primary_uom_quantity_first,
    primary_uom_quantity_current,
    primary_uom_quantity_prior,
    count_type_code,
    transaction_reason_id,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    approval_type,
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
    serial_number,
    serial_detail,
    approval_condition,
    neg_adjustment_quantity,
    neg_adjustment_amount,
    export_flag,
    parent_lpn_id,
    outermost_lpn_id,
    standard_operation_id,
    task_priority,
    cost_group_id,
    secondary_uom_quantity_first,
    secondary_uom_quantity_current,
    secondary_uom_quantity_prior,
    count_secondary_uom_first,
    count_secondary_uom_current,
    count_secondary_uom_prior,
    secondary_adjustment_quantity,
    secondary_system_qty_current,
    secondary_system_qty_first,
    secondary_system_qty_prior,
    lot_expiration_date,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.mtl_cycle_count_entries
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (CYCLE_COUNT_ENTRY_ID, kca_seq_id) IN (
      SELECT
        CYCLE_COUNT_ENTRY_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.mtl_cycle_count_entries
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        CYCLE_COUNT_ENTRY_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.mtl_cycle_count_entries SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.mtl_cycle_count_entries SET IS_DELETED_FLG = 'Y'
WHERE
  (
    CYCLE_COUNT_ENTRY_ID
  ) IN (
    SELECT
      CYCLE_COUNT_ENTRY_ID
    FROM bec_raw_dl_ext.mtl_cycle_count_entries
    WHERE
      (CYCLE_COUNT_ENTRY_ID, KCA_SEQ_ID) IN (
        SELECT
          CYCLE_COUNT_ENTRY_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.mtl_cycle_count_entries
        GROUP BY
          CYCLE_COUNT_ENTRY_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_cycle_count_entries';