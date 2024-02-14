TRUNCATE table bronze_bec_ods_stg.FND_ID_FLEX_STRUCTURES;
INSERT INTO bronze_bec_ods_stg.FND_ID_FLEX_STRUCTURES (
  application_id,
  id_flex_code,
  id_flex_num,
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
  id_flex_structure_code,
  ZD_EDITION_NAME,
  ZD_SYNC,
  KCA_OPERATION,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    application_id,
    id_flex_code,
    id_flex_num,
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
    id_flex_structure_code,
    ZD_EDITION_NAME, /*	security_group_id, */
    ZD_SYNC,
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.FND_ID_FLEX_STRUCTURES
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (APPLICATION_ID, ID_FLEX_CODE, ID_FLEX_NUM, kca_seq_id) IN (
      SELECT
        APPLICATION_ID,
        ID_FLEX_CODE,
        ID_FLEX_NUM,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.FND_ID_FLEX_STRUCTURES
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        APPLICATION_ID,
        ID_FLEX_CODE,
        ID_FLEX_NUM
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'fnd_id_flex_structures'
    )
);