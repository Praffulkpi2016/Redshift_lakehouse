DROP TABLE IF EXISTS bronze_bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY;
CREATE TABLE bronze_bec_ods_stg.GL_SEG_VAL_NORM_HIERARCHY AS
SELECT
  *
FROM bec_raw_dl_ext.GL_SEG_VAL_NORM_HIERARCHY
WHERE
  kca_operation <> 'DELETE'
  AND (flex_value_set_id, PARENT_FLEX_VALUE, CHILD_FLEX_VALUE, last_update_date) IN (
    SELECT
      flex_value_set_id,
      PARENT_FLEX_VALUE,
      CHILD_FLEX_VALUE,
      MAX(last_update_date)
    FROM bec_raw_dl_ext.GL_SEG_VAL_NORM_HIERARCHY
    WHERE
      kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') = ''
    GROUP BY
      flex_value_set_id,
      PARENT_FLEX_VALUE,
      CHILD_FLEX_VALUE
  );