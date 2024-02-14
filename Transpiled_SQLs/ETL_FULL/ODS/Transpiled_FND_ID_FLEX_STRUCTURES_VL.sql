DROP table IF EXISTS silver_bec_ods.FND_ID_FLEX_STRUCTURES_VL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.FND_ID_FLEX_STRUCTURES_VL (
  application_id DECIMAL(10, 0),
  id_flex_code STRING,
  id_flex_num DECIMAL(15, 0),
  id_flex_structure_code STRING,
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  last_update_login DECIMAL(10, 0),
  concatenated_segment_delimiter STRING,
  cross_segment_validation_flag STRING,
  dynamic_inserts_allowed_flag STRING,
  enabled_flag STRING,
  freeze_flex_definition_flag STRING,
  freeze_structured_hier_flag STRING,
  shorthand_enabled_flag STRING,
  shorthand_length DECIMAL(3, 0),
  structure_view_name STRING,
  id_flex_structure_name STRING,
  description STRING,
  shorthand_prompt STRING,
  KCA_OPERATION STRING,
  IS_DELETED_FLG STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.FND_ID_FLEX_STRUCTURES_VL (
  application_id,
  id_flex_code,
  id_flex_num,
  id_flex_structure_code,
  last_update_date,
  last_updated_by,
  creation_date,
  created_by,
  last_update_login,
  concatenated_segment_delimiter,
  cross_segment_validation_flag,
  dynamic_inserts_allowed_flag,
  enabled_flag,
  freeze_flex_definition_flag,
  freeze_structured_hier_flag,
  shorthand_enabled_flag,
  shorthand_length,
  structure_view_name,
  id_flex_structure_name,
  description,
  shorthand_prompt,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    application_id,
    id_flex_code,
    id_flex_num,
    id_flex_structure_code,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    concatenated_segment_delimiter,
    cross_segment_validation_flag,
    dynamic_inserts_allowed_flag,
    enabled_flag,
    freeze_flex_definition_flag,
    freeze_structured_hier_flag,
    shorthand_enabled_flag,
    shorthand_length,
    structure_view_name,
    id_flex_structure_name,
    description,
    shorthand_prompt,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.FND_ID_FLEX_STRUCTURES_VL
);
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'fnd_id_flex_structures_vl';