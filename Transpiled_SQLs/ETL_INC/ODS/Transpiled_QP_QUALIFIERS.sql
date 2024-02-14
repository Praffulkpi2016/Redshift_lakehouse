/* Delete Records */
DELETE FROM silver_bec_ods.QP_QUALIFIERS
WHERE
  COALESCE(QUALIFIER_ID, 0) IN (
    SELECT
      COALESCE(stg.QUALIFIER_ID, 0) AS QUALIFIER_ID
    FROM silver_bec_ods.QP_QUALIFIERS AS ods, bronze_bec_ods_stg.QP_QUALIFIERS AS stg
    WHERE
      COALESCE(ods.QUALIFIER_ID, 0) = COALESCE(stg.QUALIFIER_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.QP_QUALIFIERS (
  qualifier_id,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  last_update_login,
  qualifier_grouping_no,
  qualifier_context,
  qualifier_attribute,
  qualifier_attr_value,
  comparison_operator_code,
  excluder_flag,
  qualifier_rule_id,
  start_date_active,
  end_date_active,
  created_from_rule_id,
  qualifier_precedence,
  list_header_id,
  list_line_id,
  qualifier_datatype,
  qualifier_attr_value_to,
  context,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  attribute6,
  attribute7,
  attribute8,
  attribute9,
  attribute10,
  attribute11,
  attribute12,
  attribute13,
  attribute14,
  attribute15,
  active_flag,
  list_type_code,
  qual_attr_value_from_number,
  qual_attr_value_to_number,
  search_ind,
  qualifier_group_cnt,
  header_quals_exist_flag,
  distinct_row_count,
  others_group_cnt,
  segment_id,
  orig_sys_qualifier_ref,
  orig_sys_header_ref,
  orig_sys_line_ref,
  qualify_hier_descendents_flag,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    qualifier_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    last_update_login,
    qualifier_grouping_no,
    qualifier_context,
    qualifier_attribute,
    qualifier_attr_value,
    comparison_operator_code,
    excluder_flag,
    qualifier_rule_id,
    start_date_active,
    end_date_active,
    created_from_rule_id,
    qualifier_precedence,
    list_header_id,
    list_line_id,
    qualifier_datatype,
    qualifier_attr_value_to,
    context,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    active_flag,
    list_type_code,
    qual_attr_value_from_number,
    qual_attr_value_to_number,
    search_ind,
    qualifier_group_cnt,
    header_quals_exist_flag,
    distinct_row_count,
    others_group_cnt,
    segment_id,
    orig_sys_qualifier_ref,
    orig_sys_header_ref,
    orig_sys_line_ref,
    qualify_hier_descendents_flag,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.QP_QUALIFIERS
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(QUALIFIER_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(QUALIFIER_ID, 0) AS QUALIFIER_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.QP_QUALIFIERS
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(QUALIFIER_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.QP_QUALIFIERS SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.QP_QUALIFIERS SET IS_DELETED_FLG = 'Y'
WHERE
  (
    QUALIFIER_ID
  ) IN (
    SELECT
      QUALIFIER_ID
    FROM bec_raw_dl_ext.QP_QUALIFIERS
    WHERE
      (QUALIFIER_ID, KCA_SEQ_ID) IN (
        SELECT
          QUALIFIER_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.QP_QUALIFIERS
        GROUP BY
          QUALIFIER_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'qp_qualifiers';