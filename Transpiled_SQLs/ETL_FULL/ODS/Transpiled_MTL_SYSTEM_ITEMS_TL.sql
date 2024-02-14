DROP TABLE IF EXISTS silver_bec_ods.MTL_SYSTEM_ITEMS_TL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.MTL_SYSTEM_ITEMS_TL (
  inventory_item_id DECIMAL(15, 0),
  organization_id DECIMAL(15, 0),
  `language` STRING,
  source_lang STRING,
  description STRING,
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  long_description STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.MTL_SYSTEM_ITEMS_TL (
  inventory_item_id,
  organization_id,
  `language`,
  source_lang,
  description,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  long_description,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  inventory_item_id,
  organization_id,
  `language`,
  source_lang,
  description,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  long_description,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.MTL_SYSTEM_ITEMS_TL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'mtl_system_items_tl';