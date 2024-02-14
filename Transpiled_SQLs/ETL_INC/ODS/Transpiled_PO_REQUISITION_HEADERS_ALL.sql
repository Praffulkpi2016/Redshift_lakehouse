/* Delete Records */
DELETE FROM silver_bec_ods.PO_REQUISITION_HEADERS_ALL
WHERE
  REQUISITION_HEADER_ID IN (
    SELECT
      stg.REQUISITION_HEADER_ID
    FROM silver_bec_ods.PO_REQUISITION_HEADERS_ALL AS ods, bronze_bec_ods_stg.PO_REQUISITION_HEADERS_ALL AS stg
    WHERE
      ods.REQUISITION_HEADER_ID = stg.REQUISITION_HEADER_ID
      AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.PO_REQUISITION_HEADERS_ALL (
  requisition_header_id,
  preparer_id,
  last_update_date,
  last_updated_by,
  segment1,
  summary_flag,
  enabled_flag,
  segment2,
  segment3,
  segment4,
  segment5,
  start_date_active,
  end_date_active,
  last_update_login,
  creation_date,
  created_by,
  description,
  authorization_status,
  note_to_authorizer,
  type_lookup_code,
  transferred_to_oe_flag,
  attribute_category,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  on_line_flag,
  preliminary_research_flag,
  research_complete_flag,
  preparer_finished_flag,
  preparer_finished_date,
  agent_return_flag,
  agent_return_note,
  cancel_flag,
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
  ussgl_transaction_code,
  government_context,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  interface_source_code,
  interface_source_line_id,
  closed_code,
  org_id,
  wf_item_type,
  wf_item_key,
  emergency_po_num,
  pcard_id,
  apps_source_code,
  cbc_accounting_date,
  change_pending_flag,
  active_shopping_cart_flag,
  contractor_status,
  contractor_requisition_flag,
  supplier_notified_flag,
  emergency_po_org_id,
  approved_date,
  tax_attribute_update_code,
  first_approver_id,
  first_position_id,
  amendment_reason,
  amendment_status,
  amendment_type,
  clm_assist_contact,
  clm_assist_office,
  clm_cotr_contact,
  clm_cotr_office,
  clm_issuing_office,
  clm_mipr_acknowledged_flag,
  clm_mipr_prepared_date,
  clm_mipr_ref_num,
  clm_mipr_type,
  clm_priority_code,
  clm_req_contact,
  clm_req_office,
  conformed_header_id,
  federal_flag,
  notify_requester_flag,
  par_draft_id,
  par_flag,
  revision_num,
  suggested_award_no,
  uda_template_date,
  uda_template_id,
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    requisition_header_id,
    preparer_id,
    last_update_date,
    last_updated_by,
    segment1,
    summary_flag,
    enabled_flag,
    segment2,
    segment3,
    segment4,
    segment5,
    start_date_active,
    end_date_active,
    last_update_login,
    creation_date,
    created_by,
    description,
    authorization_status,
    note_to_authorizer,
    type_lookup_code,
    transferred_to_oe_flag,
    attribute_category,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    on_line_flag,
    preliminary_research_flag,
    research_complete_flag,
    preparer_finished_flag,
    preparer_finished_date,
    agent_return_flag,
    agent_return_note,
    cancel_flag,
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
    ussgl_transaction_code,
    government_context,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    interface_source_code,
    interface_source_line_id,
    closed_code,
    org_id,
    wf_item_type,
    wf_item_key,
    emergency_po_num,
    pcard_id,
    apps_source_code,
    cbc_accounting_date,
    change_pending_flag,
    active_shopping_cart_flag,
    contractor_status,
    contractor_requisition_flag,
    supplier_notified_flag,
    emergency_po_org_id,
    approved_date,
    tax_attribute_update_code,
    first_approver_id,
    first_position_id,
    amendment_reason,
    amendment_status,
    amendment_type,
    clm_assist_contact,
    clm_assist_office,
    clm_cotr_contact,
    clm_cotr_office,
    clm_issuing_office,
    clm_mipr_acknowledged_flag,
    clm_mipr_prepared_date,
    clm_mipr_ref_num,
    clm_mipr_type,
    clm_priority_code,
    clm_req_contact,
    clm_req_office,
    conformed_header_id,
    federal_flag,
    notify_requester_flag, /* igt_document_flag, */
    par_draft_id,
    par_flag,
    revision_num,
    suggested_award_no,
    uda_template_date,
    uda_template_id,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.PO_REQUISITION_HEADERS_ALL
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (REQUISITION_HEADER_ID, kca_seq_id) IN (
      SELECT
        REQUISITION_HEADER_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.PO_REQUISITION_HEADERS_ALL
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        REQUISITION_HEADER_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.PO_REQUISITION_HEADERS_ALL SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.PO_REQUISITION_HEADERS_ALL SET IS_DELETED_FLG = 'Y'
WHERE
  (
    REQUISITION_HEADER_ID
  ) IN (
    SELECT
      REQUISITION_HEADER_ID
    FROM bec_raw_dl_ext.PO_REQUISITION_HEADERS_ALL
    WHERE
      (REQUISITION_HEADER_ID, KCA_SEQ_ID) IN (
        SELECT
          REQUISITION_HEADER_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.PO_REQUISITION_HEADERS_ALL
        GROUP BY
          REQUISITION_HEADER_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_requisition_headers_all';