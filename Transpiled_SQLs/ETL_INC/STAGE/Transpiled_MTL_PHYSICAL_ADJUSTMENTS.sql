TRUNCATE table bronze_bec_ods_stg.MTL_PHYSICAL_ADJUSTMENTS;
INSERT INTO bronze_bec_ods_stg.MTL_PHYSICAL_ADJUSTMENTS (
  adjustment_id,
  organization_id,
  physical_inventory_id,
  inventory_item_id,
  subinventory_name,
  system_quantity,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  count_quantity,
  adjustment_quantity,
  revision,
  locator_id,
  lot_number,
  lot_expiration_date,
  serial_number,
  actual_cost,
  approval_status,
  approved_by_employee_id,
  automatic_approval_code,
  gl_adjust_account,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  lot_serial_controls,
  temp_approver,
  parent_lpn_id,
  outermost_lpn_id,
  cost_group_id,
  secondary_system_qty,
  secondary_count_qty,
  secondary_adjustment_qty,
  KCA_OPERATION,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    adjustment_id,
    organization_id,
    physical_inventory_id,
    inventory_item_id,
    subinventory_name,
    system_quantity,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    count_quantity,
    adjustment_quantity,
    revision,
    locator_id,
    lot_number,
    lot_expiration_date,
    serial_number,
    actual_cost,
    approval_status,
    approved_by_employee_id,
    automatic_approval_code,
    gl_adjust_account,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    lot_serial_controls,
    temp_approver,
    parent_lpn_id,
    outermost_lpn_id,
    cost_group_id,
    secondary_system_qty,
    secondary_count_qty,
    secondary_adjustment_qty,
    KCA_OPERATION,
    KCA_SEQ_ID,
    kca_seq_date
  FROM bec_raw_dl_ext.MTL_PHYSICAL_ADJUSTMENTS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(ADJUSTMENT_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ADJUSTMENT_ID, 0) AS ADJUSTMENT_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.MTL_PHYSICAL_ADJUSTMENTS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(ADJUSTMENT_ID, 0)
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'mtl_physical_adjustments'
      )
    )
);