DROP TABLE IF EXISTS silver_bec_ods.MTL_ABC_ASSIGNMENTS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.MTL_ABC_ASSIGNMENTS (
  inventory_item_id DECIMAL(15, 0),
  assignment_group_id DECIMAL(15, 0),
  abc_class_id DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  request_id DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.MTL_ABC_ASSIGNMENTS (
  inventory_item_id,
  assignment_group_id,
  abc_class_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  inventory_item_id,
  assignment_group_id,
  abc_class_id,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.MTL_ABC_ASSIGNMENTS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_abc_assignments';