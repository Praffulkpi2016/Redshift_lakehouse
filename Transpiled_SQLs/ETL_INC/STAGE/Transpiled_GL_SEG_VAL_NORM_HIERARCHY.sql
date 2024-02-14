TRUNCATE table bronze_bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY;
INSERT INTO bronze_bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY (
  flex_value_set_id,
  parent_flex_value,
  child_flex_value,
  summary_flag,
  last_updated_by,
  last_update_date,
  last_update_login,
  created_by,
  creation_date,
  status_code,
  kca_operation,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    flex_value_set_id,
    parent_flex_value,
    child_flex_value,
    summary_flag,
    last_updated_by,
    last_update_date,
    last_update_login,
    created_by,
    creation_date,
    status_code,
    kca_operation,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.GL_SEG_VAL_NORM_HIERARCHY
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (flex_value_set_id, PARENT_FLEX_VALUE, CHILD_FLEX_VALUE, kca_seq_id) IN (
      SELECT
        flex_value_set_id,
        PARENT_FLEX_VALUE,
        CHILD_FLEX_VALUE,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.GL_SEG_VAL_NORM_HIERARCHY
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        flex_value_set_id,
        PARENT_FLEX_VALUE,
        CHILD_FLEX_VALUE
    )
    AND (
      kca_seq_date > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'gl_seg_val_norm_hierarchy'
      )
    )
);