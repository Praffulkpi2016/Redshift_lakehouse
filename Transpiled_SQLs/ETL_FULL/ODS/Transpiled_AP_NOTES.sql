DROP TABLE IF EXISTS silver_bec_ods.AP_NOTES;
CREATE TABLE IF NOT EXISTS silver_bec_ods.AP_NOTES (
  note_id DECIMAL(15, 0),
  source_object_code STRING,
  source_object_id DECIMAL(15, 0),
  note_type STRING,
  notes_detail STRING,
  entered_by DECIMAL(15, 0),
  entered_date TIMESTAMP,
  source_lang STRING,
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  last_update_login DECIMAL(15, 0),
  NOTE_SOURCE STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.AP_NOTES (
  NOTE_ID,
  SOURCE_OBJECT_CODE,
  SOURCE_OBJECT_ID,
  NOTE_TYPE,
  NOTES_DETAIL,
  ENTERED_BY,
  ENTERED_DATE,
  SOURCE_LANG,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  NOTE_SOURCE,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
SELECT
  NOTE_ID,
  SOURCE_OBJECT_CODE,
  SOURCE_OBJECT_ID,
  NOTE_TYPE,
  NOTES_DETAIL,
  ENTERED_BY,
  ENTERED_DATE,
  SOURCE_LANG,
  CREATION_DATE,
  CREATED_BY,
  LAST_UPDATE_DATE,
  LAST_UPDATED_BY,
  LAST_UPDATE_LOGIN,
  NOTE_SOURCE,
  KCA_OPERATION,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.AP_NOTES;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'ap_notes';