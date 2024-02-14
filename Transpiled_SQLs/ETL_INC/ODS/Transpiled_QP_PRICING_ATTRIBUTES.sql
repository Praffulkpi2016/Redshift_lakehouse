/* Delete Records */
DELETE FROM silver_bec_ods.QP_PRICING_ATTRIBUTES
WHERE
  (
    COALESCE(PRICING_ATTRIBUTE_ID, 0)
  ) IN (
    SELECT
      COALESCE(stg.PRICING_ATTRIBUTE_ID, 0) AS PRICING_ATTRIBUTE_ID
    FROM silver_bec_ods.QP_PRICING_ATTRIBUTES AS ods, bronze_bec_ods_stg.QP_PRICING_ATTRIBUTES AS stg
    WHERE
      COALESCE(ods.PRICING_ATTRIBUTE_ID, 0) = COALESCE(stg.PRICING_ATTRIBUTE_ID, 0)
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.QP_PRICING_ATTRIBUTES (
  pricing_attribute_id,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  program_application_id,
  program_id,
  program_update_date,
  request_id,
  list_line_id,
  excluder_flag,
  accumulate_flag,
  product_attribute_context,
  product_attribute,
  product_attr_value,
  product_uom_code,
  pricing_attribute_context,
  pricing_attribute,
  pricing_attr_value_from,
  pricing_attr_value_to,
  attribute_grouping_no,
  product_attribute_datatype,
  pricing_attribute_datatype,
  comparison_operator_code,
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
  list_header_id,
  pricing_phase_id,
  qualification_ind,
  pricing_attr_value_from_number,
  pricing_attr_value_to_number,
  distinct_row_count,
  search_ind,
  pattern_value_from_positive,
  pattern_value_to_positive,
  pattern_value_from_negative,
  pattern_value_to_negative,
  product_segment_id,
  pricing_segment_id,
  orig_sys_line_ref,
  orig_sys_pricing_attr_ref,
  orig_sys_header_ref,
  kca_operation,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    pricing_attribute_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    program_application_id,
    program_id,
    program_update_date,
    request_id,
    list_line_id,
    excluder_flag,
    accumulate_flag,
    product_attribute_context,
    product_attribute,
    product_attr_value,
    product_uom_code,
    pricing_attribute_context,
    pricing_attribute,
    pricing_attr_value_from,
    pricing_attr_value_to,
    attribute_grouping_no,
    product_attribute_datatype,
    pricing_attribute_datatype,
    comparison_operator_code,
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
    list_header_id,
    pricing_phase_id,
    qualification_ind,
    pricing_attr_value_from_number,
    pricing_attr_value_to_number,
    distinct_row_count,
    search_ind,
    pattern_value_from_positive,
    pattern_value_to_positive,
    pattern_value_from_negative,
    pattern_value_to_negative,
    product_segment_id,
    pricing_segment_id,
    orig_sys_line_ref,
    orig_sys_pricing_attr_ref,
    orig_sys_header_ref,
    kca_operation,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    kca_seq_date
  FROM bronze_bec_ods_stg.QP_PRICING_ATTRIBUTES
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(PRICING_ATTRIBUTE_ID, 0), KCA_SEQ_ID) IN (
      SELECT
        COALESCE(PRICING_ATTRIBUTE_ID, 0) AS PRICING_ATTRIBUTE_ID,
        MAX(KCA_SEQ_ID)
      FROM bronze_bec_ods_stg.QP_PRICING_ATTRIBUTES
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        PRICING_ATTRIBUTE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.QP_PRICING_ATTRIBUTES SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.QP_PRICING_ATTRIBUTES SET IS_DELETED_FLG = 'Y'
WHERE
  (
    PRICING_ATTRIBUTE_ID
  ) IN (
    SELECT
      PRICING_ATTRIBUTE_ID
    FROM bec_raw_dl_ext.QP_PRICING_ATTRIBUTES
    WHERE
      (PRICING_ATTRIBUTE_ID, KCA_SEQ_ID) IN (
        SELECT
          PRICING_ATTRIBUTE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.QP_PRICING_ATTRIBUTES
        GROUP BY
          PRICING_ATTRIBUTE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'qp_pricing_attributes';