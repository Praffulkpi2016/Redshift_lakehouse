DROP TABLE IF EXISTS silver_bec_ods.PO_HAZARD_CLASSES_TL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.PO_HAZARD_CLASSES_TL (
  hazard_class_id DECIMAL(15, 0),
  `language` STRING,
  source_lang STRING,
  hazard_class STRING,
  description STRING,
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_updated_by DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_update_login DECIMAL(15, 0),
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.PO_HAZARD_CLASSES_TL (
  hazard_class_id,
  `language`,
  source_lang,
  hazard_class,
  description,
  creation_date,
  created_by,
  last_updated_by,
  last_update_date,
  last_update_login,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
SELECT
  hazard_class_id,
  `language`,
  source_lang,
  hazard_class,
  description,
  creation_date,
  created_by,
  last_updated_by,
  last_update_date,
  last_update_login,
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.PO_HAZARD_CLASSES_TL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_hazard_classes_tl';