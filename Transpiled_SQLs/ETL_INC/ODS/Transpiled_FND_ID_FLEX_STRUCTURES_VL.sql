TRUNCATE table silver_bec_ods.FND_ID_FLEX_STRUCTURES_VL;
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