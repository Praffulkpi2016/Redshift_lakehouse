/* Delete Records */
DELETE FROM silver_bec_ods.QP_LIST_HEADERS_B
WHERE
  COALESCE(LIST_HEADER_ID, 0) IN (
    SELECT
      COALESCE(stg.LIST_HEADER_ID, 0) AS LIST_HEADER_ID
    FROM silver_bec_ods.QP_LIST_HEADERS_B AS ods, bronze_bec_ods_stg.QP_LIST_HEADERS_B AS stg
    WHERE
      ods.LIST_HEADER_ID = stg.LIST_HEADER_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.QP_LIST_HEADERS_B (
  list_header_id,
  creation_date,
  created_by,
  last_update_date,
  last_updated_by,
  last_update_login,
  program_application_id,
  program_id,
  program_update_date,
  request_id,
  list_type_code,
  start_date_active,
  end_date_active,
  automatic_flag,
  currency_code,
  rounding_factor,
  ship_method_code,
  freight_terms_code,
  terms_id,
  comments,
  discount_lines_flag,
  gsa_indicator,
  prorate_flag,
  source_system_code,
  ask_for_flag,
  active_flag,
  parent_list_header_id,
  start_date_active_first,
  end_date_active_first,
  active_date_first_type,
  start_date_active_second,
  end_date_active_second,
  active_date_second_type,
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
  limit_exists_flag,
  mobile_download,
  currency_header_id,
  pte_code,
  list_source_code,
  orig_system_header_ref,
  orig_org_id,
  global_flag,
  shareable_flag,
  sold_to_org_id,
  locked_from_list_header_id,
  KCA_OPERATION,
  IS_DELETED_FLG,
  KCA_SEQ_ID,
  kca_seq_date
)
(
  SELECT
    list_header_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    program_application_id,
    program_id,
    program_update_date,
    request_id,
    list_type_code,
    start_date_active,
    end_date_active,
    automatic_flag,
    currency_code,
    rounding_factor,
    ship_method_code,
    freight_terms_code,
    terms_id,
    comments,
    discount_lines_flag,
    gsa_indicator,
    prorate_flag,
    source_system_code,
    ask_for_flag,
    active_flag,
    parent_list_header_id,
    start_date_active_first,
    end_date_active_first,
    active_date_first_type,
    start_date_active_second,
    end_date_active_second,
    active_date_second_type,
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
    limit_exists_flag,
    mobile_download,
    currency_header_id,
    pte_code,
    list_source_code,
    orig_system_header_ref,
    orig_org_id,
    global_flag,
    shareable_flag,
    sold_to_org_id,
    locked_from_list_header_id,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.QP_LIST_HEADERS_B
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (COALESCE(LIST_HEADER_ID, 0), kca_seq_id) IN (
      SELECT
        COALESCE(LIST_HEADER_ID, 0) AS LIST_HEADER_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.QP_LIST_HEADERS_B
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        COALESCE(LIST_HEADER_ID, 0)
    )
);
/* Soft delete */
UPDATE silver_bec_ods.QP_LIST_HEADERS_B SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.QP_LIST_HEADERS_B SET IS_DELETED_FLG = 'Y'
WHERE
  (
    LIST_HEADER_ID
  ) IN (
    SELECT
      LIST_HEADER_ID
    FROM bec_raw_dl_ext.QP_LIST_HEADERS_B
    WHERE
      (LIST_HEADER_ID, KCA_SEQ_ID) IN (
        SELECT
          LIST_HEADER_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.QP_LIST_HEADERS_B
        GROUP BY
          LIST_HEADER_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'qp_list_headers_b';