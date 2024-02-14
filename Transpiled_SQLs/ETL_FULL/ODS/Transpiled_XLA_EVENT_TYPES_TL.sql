DROP TABLE IF EXISTS silver_bec_ods.xla_event_types_tl;
CREATE TABLE IF NOT EXISTS silver_bec_ods.xla_event_types_tl (
  APPLICATION_ID DECIMAL(15, 0),
  ENTITY_CODE STRING,
  EVENT_CLASS_CODE STRING,
  EVENT_TYPE_CODE STRING,
  LANGUAGE STRING,
  NAME STRING,
  DESCRIPTION STRING,
  SOURCE_LANG STRING,
  CREATION_DATE TIMESTAMP,
  CREATED_BY DECIMAL(15, 0),
  LAST_UPDATE_DATE TIMESTAMP,
  LAST_UPDATED_BY DECIMAL(15, 0),
  LAST_UPDATE_LOGIN DECIMAL(15, 0),
  ZD_EDITION_NAME STRING,
  ZD_SYNC STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.xla_event_types_tl (
  APPLICATION_ID,
  ENTITY_CODE,
  EVENT_CLASS_CODE,
  EVENT_TYPE_CODE,
  LANGUAGE,
  NAME,
  DESCRIPTION,
  SOURCE_LANG,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  APPLICATION_ID,
  ENTITY_CODE,
  EVENT_CLASS_CODE,
  EVENT_TYPE_CODE,
  LANGUAGE,
  NAME,
  DESCRIPTION,
  SOURCE_LANG,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.xla_event_types_tl;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'xla_event_types_tl';