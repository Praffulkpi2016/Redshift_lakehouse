/* Delete Records */
DELETE FROM silver_bec_ods.MTL_PHYSICAL_ADJUSTMENTS
WHERE
  (
    COALESCE(ADJUSTMENT_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.ADJUSTMENT_ID, 0) AS ADJUSTMENT_ID
    FROM silver_bec_ods.MTL_PHYSICAL_ADJUSTMENTS AS ods, bronze_bec_ods_stg.MTL_PHYSICAL_ADJUSTMENTS AS stg
    WHERE
      COALESCE(ods.ADJUSTMENT_ID, 0) = COALESCE(stg.ADJUSTMENT_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MTL_PHYSICAL_ADJUSTMENTS (
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
  IS_DELETED_FLG,
  kca_seq_id,
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
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MTL_PHYSICAL_ADJUSTMENTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(ADJUSTMENT_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(ADJUSTMENT_ID, 0) AS ADJUSTMENT_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MTL_PHYSICAL_ADJUSTMENTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(ADJUSTMENT_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MTL_PHYSICAL_ADJUSTMENTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MTL_PHYSICAL_ADJUSTMENTS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    ADJUSTMENT_ID
  ) IN (
    SELECT
      ADJUSTMENT_ID
    FROM bec_raw_dl_ext.MTL_PHYSICAL_ADJUSTMENTS
    WHERE
      (ADJUSTMENT_ID, KCA_SEQ_ID) IN (
        SELECT
          ADJUSTMENT_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MTL_PHYSICAL_ADJUSTMENTS
        GROUP BY
          ADJUSTMENT_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_physical_adjustments';