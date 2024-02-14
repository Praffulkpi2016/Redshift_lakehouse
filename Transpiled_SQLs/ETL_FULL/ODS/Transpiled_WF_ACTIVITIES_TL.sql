DROP TABLE IF EXISTS silver_bec_ods.WF_ACTIVITIES_TL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.WF_ACTIVITIES_TL (
  item_type STRING,
  name STRING,
  version DECIMAL(15, 0),
  display_name STRING,
  `language` STRING,
  protect_level DECIMAL(15, 0),
  custom_level DECIMAL(15, 0),
  description STRING,
  source_lang STRING,
  security_group_id STRING,
  zd_edition_name STRING,
  zd_sync STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.WF_ACTIVITIES_TL (
  item_type,
  `name`,
  version,
  display_name,
  `language`,
  protect_level,
  custom_level,
  description,
  source_lang,
  security_group_id,
  zd_edition_name,
  zd_sync,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  item_type,
  `name`,
  version,
  display_name,
  `language`,
  protect_level,
  custom_level,
  description,
  source_lang,
  security_group_id,
  zd_edition_name,
  zd_sync,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.WF_ACTIVITIES_TL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'wf_activities_tl';