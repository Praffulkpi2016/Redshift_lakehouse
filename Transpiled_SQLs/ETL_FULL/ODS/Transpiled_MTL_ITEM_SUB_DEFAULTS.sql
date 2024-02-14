DROP TABLE IF EXISTS silver_bec_ods.MTL_ITEM_SUB_DEFAULTS;
CREATE TABLE IF NOT EXISTS silver_bec_ods.MTL_ITEM_SUB_DEFAULTS (
  inventory_item_id DECIMAL(15, 0),
  organization_id DECIMAL(15, 0),
  subinventory_code STRING,
  default_type DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  request_id DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.MTL_ITEM_SUB_DEFAULTS (
  inventory_item_id,
  organization_id,
  subinventory_code,
  default_type,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
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
  organization_id,
  subinventory_code,
  default_type,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.MTL_ITEM_SUB_DEFAULTS;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_item_sub_defaults';