/* Delete Records */
DELETE FROM silver_bec_ods.BOM_COMPONENTS_B
WHERE
  COMPONENT_SEQUENCE_ID IN (
    SELECT
      stg.COMPONENT_SEQUENCE_ID
    FROM silver_bec_ods.BOM_COMPONENTS_B AS ods, bronze_bec_ods_stg.BOM_COMPONENTS_B AS stg
    WHERE
      ods.COMPONENT_SEQUENCE_ID = stg.COMPONENT_SEQUENCE_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.BOM_COMPONENTS_B (
  operation_seq_num,
  component_item_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  item_num,
  component_quantity,
  component_yield_factor,
  component_remarks,
  effectivity_date,
  change_notice,
  implementation_date,
  disable_date,
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
  planning_factor,
  quantity_related,
  so_basis,
  optional,
  mutually_exclusive_options,
  include_in_cost_rollup,
  check_atp,
  shipping_allowed,
  required_to_ship,
  required_for_revenue,
  include_on_ship_docs,
  include_on_bill_docs,
  low_quantity,
  high_quantity,
  acd_type,
  old_component_sequence_id,
  component_sequence_id,
  bill_sequence_id,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  wip_supply_type,
  pick_components,
  supply_subinventory,
  supply_locator_id,
  operation_lead_time_percent,
  revised_item_sequence_id,
  cost_factor,
  bom_item_type,
  from_end_item_unit_number,
  to_end_item_unit_number,
  original_system_reference,
  eco_for_production,
  enforce_int_requirements,
  component_item_revision_id,
  delete_group_name,
  dg_description,
  optional_on_model,
  parent_bill_seq_id,
  model_comp_seq_id,
  plan_level,
  from_bill_revision_id,
  to_bill_revision_id,
  auto_request_material,
  suggested_vendor_name,
  vendor_id,
  unit_price,
  obj_name,
  pk1_value,
  pk2_value,
  pk3_value,
  pk4_value,
  pk5_value,
  from_end_item_rev_id,
  to_end_item_rev_id,
  overlapping_changes,
  from_object_revision_id,
  from_minor_revision_id,
  to_object_revision_id,
  to_minor_revision_id,
  from_end_item_minor_rev_id,
  to_end_item_minor_rev_id,
  component_minor_revision_id,
  from_structure_revision_code,
  to_structure_revision_code,
  from_end_item_strc_rev_id,
  to_end_item_strc_rev_id,
  basis_type,
  common_component_sequence_id,
  inherit_flag,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    operation_seq_num,
    component_item_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    item_num,
    component_quantity,
    component_yield_factor,
    component_remarks,
    effectivity_date,
    change_notice,
    implementation_date,
    disable_date,
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
    planning_factor,
    quantity_related,
    so_basis,
    optional,
    mutually_exclusive_options,
    include_in_cost_rollup,
    check_atp,
    shipping_allowed,
    required_to_ship,
    required_for_revenue,
    include_on_ship_docs,
    include_on_bill_docs,
    low_quantity,
    high_quantity,
    acd_type,
    old_component_sequence_id,
    component_sequence_id,
    bill_sequence_id,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    wip_supply_type,
    pick_components,
    supply_subinventory,
    supply_locator_id,
    operation_lead_time_percent,
    revised_item_sequence_id,
    cost_factor,
    bom_item_type,
    from_end_item_unit_number,
    to_end_item_unit_number,
    original_system_reference,
    eco_for_production,
    enforce_int_requirements,
    component_item_revision_id,
    delete_group_name,
    dg_description,
    optional_on_model,
    parent_bill_seq_id,
    model_comp_seq_id,
    plan_level,
    from_bill_revision_id,
    to_bill_revision_id,
    auto_request_material,
    suggested_vendor_name,
    vendor_id,
    unit_price,
    obj_name,
    pk1_value,
    pk2_value,
    pk3_value,
    pk4_value,
    pk5_value,
    from_end_item_rev_id,
    to_end_item_rev_id,
    overlapping_changes,
    from_object_revision_id,
    from_minor_revision_id,
    to_object_revision_id,
    to_minor_revision_id,
    from_end_item_minor_rev_id,
    to_end_item_minor_rev_id,
    component_minor_revision_id,
    from_structure_revision_code,
    to_structure_revision_code,
    from_end_item_strc_rev_id,
    to_end_item_strc_rev_id,
    basis_type,
    common_component_sequence_id,
    inherit_flag,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.BOM_COMPONENTS_B
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COMPONENT_SEQUENCE_ID, kca_seq_id) IN (
      SELECT
        COMPONENT_SEQUENCE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.BOM_COMPONENTS_B
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COMPONENT_SEQUENCE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.BOM_COMPONENTS_B SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.BOM_COMPONENTS_B SET IS_DELETED_FLG = 'Y'
WHERE
  (
    COMPONENT_SEQUENCE_ID
  ) IN (
    SELECT
      COMPONENT_SEQUENCE_ID
    FROM bec_raw_dl_ext.BOM_COMPONENTS_B
    WHERE
      (COMPONENT_SEQUENCE_ID, KCA_SEQ_ID) IN (
        SELECT
          COMPONENT_SEQUENCE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.BOM_COMPONENTS_B
        GROUP BY
          COMPONENT_SEQUENCE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'bom_components_b';