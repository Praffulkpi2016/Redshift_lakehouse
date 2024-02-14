/* Delete Records */
DELETE FROM silver_bec_ods.GL_SEG_VAL_NORM_HIERARCHY
WHERE
  (flex_value_set_id, PARENT_FLEX_VALUE, CHILD_FLEX_VALUE) IN (
    SELECT
      stg.flex_value_set_id,
      stg.PARENT_FLEX_VALUE,
      stg.CHILD_FLEX_VALUE
    FROM silver_bec_ods.GL_SEG_VAL_NORM_HIERARCHY AS ods, bronze_bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY AS stg
    WHERE
      ods.flex_value_set_id = stg.flex_value_set_id
      AND ods.PARENT_FLEX_VALUE = stg.PARENT_FLEX_VALUE
      AND ods.CHILD_FLEX_VALUE = stg.CHILD_FLEX_VALUE
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.GL_SEG_VAL_NORM_HIERARCHY (
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
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
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
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY
WHERE
  kca_operation IN ('INSERT', 'UPDATE')
  AND (flex_value_set_id, PARENT_FLEX_VALUE, CHILD_FLEX_VALUE, kca_seq_id) IN (
    SELECT
      flex_value_set_id,
      PARENT_FLEX_VALUE,
      CHILD_FLEX_VALUE,
      MAX(kca_seq_id)
    FROM bronze_bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY
    WHERE
      kca_operation IN ('INSERT', 'UPDATE')
    GROUP BY
      flex_value_set_id,
      PARENT_FLEX_VALUE,
      CHILD_FLEX_VALUE
  );
/* Soft delete */
UPDATE silver_bec_ods.GL_SEG_VAL_NORM_HIERARCHY SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.GL_SEG_VAL_NORM_HIERARCHY SET IS_DELETED_FLG = 'Y'
WHERE
  (flex_value_set_id, PARENT_FLEX_VALUE, CHILD_FLEX_VALUE) IN (
    SELECT
      flex_value_set_id,
      PARENT_FLEX_VALUE,
      CHILD_FLEX_VALUE
    FROM bec_raw_dl_ext.GL_SEG_VAL_NORM_HIERARCHY
    WHERE
      (flex_value_set_id, PARENT_FLEX_VALUE, CHILD_FLEX_VALUE, KCA_SEQ_ID) IN (
        SELECT
          flex_value_set_id,
          PARENT_FLEX_VALUE,
          CHILD_FLEX_VALUE,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.GL_SEG_VAL_NORM_HIERARCHY
        GROUP BY
          flex_value_set_id,
          PARENT_FLEX_VALUE,
          CHILD_FLEX_VALUE
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'gl_seg_val_norm_hierarchy';