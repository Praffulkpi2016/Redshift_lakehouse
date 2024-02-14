DROP TABLE IF EXISTS silver_bec_ods.BOM_STRUCTURES_B;
CREATE TABLE IF NOT EXISTS silver_bec_ods.bom_structures_b (
  assembly_item_id DECIMAL(15, 0),
  organization_id DECIMAL(15, 0),
  alternate_bom_designator STRING,
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  common_assembly_item_id DECIMAL(15, 0),
  specific_assembly_comment STRING,
  pending_from_ecn STRING,
  attribute_category STRING,
  attribute1 STRING,
  attribute2 STRING,
  attribute3 STRING,
  attribute4 STRING,
  attribute5 STRING,
  attribute6 STRING,
  attribute7 STRING,
  attribute8 STRING,
  attribute9 STRING,
  attribute10 STRING,
  attribute11 STRING,
  attribute12 STRING,
  attribute13 STRING,
  attribute14 STRING,
  attribute15 STRING,
  assembly_type DECIMAL(15, 0),
  common_bill_sequence_id DECIMAL(15, 0),
  bill_sequence_id DECIMAL(15, 0),
  request_id DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  common_organization_id DECIMAL(15, 0),
  next_explode_date TIMESTAMP,
  project_id DECIMAL(15, 0),
  task_id DECIMAL(15, 0),
  original_system_reference STRING,
  structure_type_id DECIMAL(15, 0),
  implementation_date TIMESTAMP,
  obj_name STRING,
  pk1_value STRING,
  pk2_value STRING,
  pk3_value STRING,
  pk4_value STRING,
  pk5_value STRING,
  effectivity_control DECIMAL(15, 0),
  is_preferred STRING,
  source_bill_sequence_id DECIMAL(15, 0),
  KCA_OPERATION STRING,
  `IS_DELETED_FLG` STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.bom_structures_b (
  assembly_item_id,
  organization_id,
  alternate_bom_designator,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  common_assembly_item_id,
  specific_assembly_comment,
  pending_from_ecn,
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
  assembly_type,
  common_bill_sequence_id,
  bill_sequence_id,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  common_organization_id,
  next_explode_date,
  project_id,
  task_id,
  original_system_reference,
  structure_type_id,
  implementation_date,
  obj_name,
  pk1_value,
  pk2_value,
  pk3_value,
  pk4_value,
  pk5_value,
  effectivity_control,
  is_preferred,
  source_bill_sequence_id,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  assembly_item_id,
  organization_id,
  alternate_bom_designator,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  common_assembly_item_id,
  specific_assembly_comment,
  pending_from_ecn,
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
  assembly_type,
  common_bill_sequence_id,
  bill_sequence_id,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  common_organization_id,
  next_explode_date,
  project_id,
  task_id,
  original_system_reference,
  structure_type_id,
  implementation_date,
  obj_name,
  pk1_value,
  pk2_value,
  pk3_value,
  pk4_value,
  pk5_value,
  effectivity_control,
  is_preferred,
  source_bill_sequence_id,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(kca_seq_id, '') AS DECIMAL(36, 0)) AS kca_seq_id,
  kca_seq_date
FROM bronze_bec_ods_stg.bom_structures_b;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'bom_structures_b';