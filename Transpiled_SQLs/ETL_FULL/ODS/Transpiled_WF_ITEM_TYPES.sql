DROP TABLE IF EXISTS silver_bec_ods.wf_item_types;
CREATE TABLE IF NOT EXISTS silver_bec_ods.wf_item_types (
  NAME STRING,
  PROTECT_LEVEL DECIMAL(28, 10),
  CUSTOM_LEVEL DECIMAL(28, 10),
  WF_SELECTOR STRING,
  READ_ROLE STRING,
  WRITE_ROLE STRING,
  EXECUTE_ROLE STRING,
  PERSISTENCE_TYPE STRING,
  PERSISTENCE_DAYS DECIMAL(28, 10),
  SECURITY_GROUP_ID STRING,
  NUM_ACTIVE DECIMAL(28, 10),
  NUM_ERROR DECIMAL(28, 10),
  NUM_DEFER DECIMAL(28, 10),
  NUM_SUSPEND DECIMAL(28, 10),
  NUM_COMPLETE DECIMAL(28, 10),
  NUM_PURGEABLE DECIMAL(28, 10),
  ZD_EDITION_NAME STRING,
  ZD_SYNC STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  KCA_SEQ_ID DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.wf_item_types (
  NAME,
  PROTECT_LEVEL,
  CUSTOM_LEVEL,
  WF_SELECTOR,
  READ_ROLE,
  WRITE_ROLE,
  EXECUTE_ROLE,
  PERSISTENCE_TYPE,
  PERSISTENCE_DAYS,
  SECURITY_GROUP_ID,
  NUM_ACTIVE,
  NUM_ERROR,
  NUM_DEFER,
  NUM_SUSPEND,
  NUM_COMPLETE,
  NUM_PURGEABLE,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
SELECT
  NAME,
  PROTECT_LEVEL,
  CUSTOM_LEVEL,
  WF_SELECTOR,
  READ_ROLE,
  WRITE_ROLE,
  EXECUTE_ROLE,
  PERSISTENCE_TYPE,
  PERSISTENCE_DAYS,
  SECURITY_GROUP_ID,
  NUM_ACTIVE,
  NUM_ERROR,
  NUM_DEFER,
  NUM_SUSPEND,
  NUM_COMPLETE,
  NUM_PURGEABLE,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.wf_item_types;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'wf_item_types';