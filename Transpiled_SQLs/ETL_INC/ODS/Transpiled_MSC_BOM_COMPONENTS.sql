/* Delete Records   */
DELETE FROM silver_bec_ods.MSC_BOM_COMPONENTS
WHERE
  (PLAN_ID, COMPONENT_SEQUENCE_ID, SR_INSTANCE_ID, BILL_SEQUENCE_ID) IN (
    SELECT
      stg.PLAN_ID,
      stg.COMPONENT_SEQUENCE_ID,
      stg.SR_INSTANCE_ID,
      stg.BILL_SEQUENCE_ID
    FROM silver_bec_ods.MSC_BOM_COMPONENTS AS ods, bronze_bec_ods_stg.MSC_BOM_COMPONENTS AS stg
    WHERE
      ods.PLAN_ID = stg.PLAN_ID
      AND ods.COMPONENT_SEQUENCE_ID = stg.COMPONENT_SEQUENCE_ID
      AND ods.SR_INSTANCE_ID = stg.SR_INSTANCE_ID
      AND ods.BILL_SEQUENCE_ID = stg.BILL_SEQUENCE_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.MSC_BOM_COMPONENTS (
  plan_id,
  component_sequence_id,
  bill_sequence_id,
  sr_instance_id,
  organization_id,
  inventory_item_id,
  using_assembly_id,
  component_type,
  scaling_type,
  change_notice,
  revision,
  uom_code,
  usage_quantity,
  effectivity_date,
  disable_date,
  from_unit_number,
  to_unit_number,
  use_up_code,
  suggested_effectivity_date,
  driving_item_id,
  operation_offset_percent,
  optional_component,
  old_effectivity_date,
  wip_supply_type,
  planning_factor,
  atp_flag,
  component_yield_factor,
  refresh_number,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
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
  scale_multiple,
  scale_rounding_variance,
  rounding_direction,
  primary_flag,
  contribute_to_step_qty,
  old_component_sequence_id,
  operation_seq_num,
  new_plan_id,
  new_plan_list,
  applied,
  simulation_set_id,
  base_model_item_id,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    plan_id,
    component_sequence_id,
    bill_sequence_id,
    sr_instance_id,
    organization_id,
    inventory_item_id,
    using_assembly_id,
    component_type,
    scaling_type,
    change_notice,
    revision,
    uom_code,
    usage_quantity,
    effectivity_date,
    disable_date,
    from_unit_number,
    to_unit_number,
    use_up_code,
    suggested_effectivity_date,
    driving_item_id,
    operation_offset_percent,
    optional_component,
    old_effectivity_date,
    wip_supply_type,
    planning_factor,
    atp_flag,
    component_yield_factor,
    refresh_number,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
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
    scale_multiple,
    scale_rounding_variance,
    rounding_direction,
    primary_flag,
    contribute_to_step_qty,
    old_component_sequence_id,
    operation_seq_num,
    new_plan_id,
    new_plan_list,
    applied,
    simulation_set_id,
    base_model_item_id,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.MSC_BOM_COMPONENTS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (PLAN_ID, COMPONENT_SEQUENCE_ID, SR_INSTANCE_ID, BILL_SEQUENCE_ID, KCA_SEQ_ID) IN (
      SELECT
        PLAN_ID,
        COMPONENT_SEQUENCE_ID,
        SR_INSTANCE_ID,
        BILL_SEQUENCE_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.MSC_BOM_COMPONENTS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        PLAN_ID,
        COMPONENT_SEQUENCE_ID,
        SR_INSTANCE_ID,
        BILL_SEQUENCE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.MSC_BOM_COMPONENTS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.MSC_BOM_COMPONENTS SET IS_DELETED_FLG = 'Y'
WHERE
  (PLAN_ID, COMPONENT_SEQUENCE_ID, SR_INSTANCE_ID, BILL_SEQUENCE_ID) IN (
    SELECT
      PLAN_ID,
      COMPONENT_SEQUENCE_ID,
      SR_INSTANCE_ID,
      BILL_SEQUENCE_ID
    FROM bec_raw_dl_ext.MSC_BOM_COMPONENTS
    WHERE
      (PLAN_ID, COMPONENT_SEQUENCE_ID, SR_INSTANCE_ID, BILL_SEQUENCE_ID, KCA_SEQ_ID) IN (
        SELECT
          PLAN_ID,
          COMPONENT_SEQUENCE_ID,
          SR_INSTANCE_ID,
          BILL_SEQUENCE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.MSC_BOM_COMPONENTS
        GROUP BY
          PLAN_ID,
          COMPONENT_SEQUENCE_ID,
          SR_INSTANCE_ID,
          BILL_SEQUENCE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'msc_bom_components';