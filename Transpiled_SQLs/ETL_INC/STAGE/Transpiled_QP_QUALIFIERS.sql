TRUNCATE table
	table bronze_bec_ods_stg.QP_QUALIFIERS;
INSERT INTO bronze_bec_ods_stg.QP_QUALIFIERS (
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
  KCA_OPERATION,
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
    KCA_OPERATION,
    kca_seq_id,
    kca_seq_date
  FROM bec_raw_dl_ext.QP_QUALIFIERS
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (COALESCE(QUALIFIER_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(QUALIFIER_ID, 0) AS QUALIFIER_ID,
        MAX(KCA_SEQ_ID)
      FROM bec_raw_dl_ext.QP_QUALIFIERS
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        COALESCE(QUALIFIER_ID, 0)
    )
    AND kca_seq_date > (
      SELECT
        (
          executebegints - prune_days
        )
      FROM bec_etl_ctrl.batch_ods_info
      WHERE
        ods_table_name = 'qp_qualifiers'
    )
);