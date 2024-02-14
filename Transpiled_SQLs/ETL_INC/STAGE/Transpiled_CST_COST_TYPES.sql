TRUNCATE table bronze_bec_ods_stg.CST_COST_TYPES;
INSERT INTO bronze_bec_ods_stg.CST_COST_TYPES (
  cost_type_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  organization_id,
  cost_type,
  description,
  costing_method_type,
  frozen_standard_flag,
  default_cost_type_id,
  bom_snapshot_flag,
  alternate_bom_designator,
  allow_updates_flag,
  pl_element_flag,
  pl_resource_flag,
  pl_operation_flag,
  pl_activity_flag,
  disable_date,
  available_to_eng_flag,
  component_yield_flag,
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
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    cost_type_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    organization_id,
    cost_type,
    description,
    costing_method_type,
    frozen_standard_flag,
    default_cost_type_id,
    bom_snapshot_flag,
    alternate_bom_designator,
    allow_updates_flag,
    pl_element_flag,
    pl_resource_flag,
    pl_operation_flag,
    pl_activity_flag,
    disable_date,
    available_to_eng_flag,
    component_yield_flag,
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
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    ZD_EDITION_NAME,
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.CST_COST_TYPES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(COST_TYPE_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(COST_TYPE_ID, 0) AS COST_TYPE_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.CST_COST_TYPES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(COST_TYPE_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'cst_cost_types'
    )
);