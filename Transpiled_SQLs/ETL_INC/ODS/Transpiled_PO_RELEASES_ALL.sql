/* Delete Records */
DELETE FROM silver_bec_ods.po_releases_all
WHERE
  PO_RELEASE_ID IN (
    SELECT
      stg.PO_RELEASE_ID
    FROM silver_bec_ods.po_releases_all AS ods, bronze_bec_ods_stg.po_releases_all AS stg
    WHERE
      ods.PO_RELEASE_ID = stg.PO_RELEASE_ID AND stg.kca_operation IN ('INSERT', 'UPDATE')
  );
/* Insert records */
INSERT INTO silver_bec_ods.po_releases_all (
  po_release_id,
  last_update_date,
  last_updated_by,
  po_header_id,
  release_num,
  agent_id,
  release_date,
  last_update_login,
  creation_date,
  created_by,
  revision_num,
  revised_date,
  approved_flag,
  approved_date,
  print_count,
  printed_date,
  acceptance_required_flag,
  acceptance_due_date,
  hold_by,
  hold_date,
  hold_reason,
  hold_flag,
  cancel_flag,
  cancelled_by,
  cancel_date,
  cancel_reason,
  firm_status_lookup_code,
  firm_date,
  attribute_category,
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
  authorization_status,
  ussgl_transaction_code,
  government_context,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  closed_code,
  frozen_flag,
  release_type,
  note_to_vendor,
  org_id,
  edi_processed_flag,
  global_attribute_category,
  global_attribute1,
  global_attribute2,
  global_attribute3,
  global_attribute4,
  global_attribute5,
  global_attribute6,
  global_attribute7,
  global_attribute8,
  global_attribute9,
  global_attribute10,
  global_attribute11,
  global_attribute12,
  global_attribute13,
  global_attribute14,
  global_attribute15,
  global_attribute16,
  global_attribute17,
  global_attribute18,
  global_attribute19,
  global_attribute20,
  wf_item_type,
  wf_item_key,
  pcard_id,
  pay_on_code,
  xml_flag,
  xml_send_date,
  xml_change_send_date,
  consigned_consumption_flag,
  cbc_accounting_date,
  change_requested_by,
  shipping_control,
  change_summary,
  vendor_order_num,
  document_creation_method,
  submit_date,
  tax_attribute_update_code,
  comm_rev_num,
  otm_status_code,
  otm_recovery_flag,
  `cancel_reason#1`,
  KCA_OPERATION,
  IS_DELETED_FLG,
  kca_seq_id,
  kca_seq_date
)
(
  SELECT
    po_release_id,
    last_update_date,
    last_updated_by,
    po_header_id,
    release_num,
    agent_id,
    release_date,
    last_update_login,
    creation_date,
    created_by,
    revision_num,
    revised_date,
    approved_flag,
    approved_date,
    print_count,
    printed_date,
    acceptance_required_flag,
    acceptance_due_date,
    hold_by,
    hold_date,
    hold_reason,
    hold_flag,
    cancel_flag,
    cancelled_by,
    cancel_date,
    cancel_reason,
    firm_status_lookup_code,
    firm_date,
    attribute_category,
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
    authorization_status,
    ussgl_transaction_code,
    government_context,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    closed_code,
    frozen_flag,
    release_type,
    note_to_vendor,
    org_id,
    edi_processed_flag,
    global_attribute_category,
    global_attribute1,
    global_attribute2,
    global_attribute3,
    global_attribute4,
    global_attribute5,
    global_attribute6,
    global_attribute7,
    global_attribute8,
    global_attribute9,
    global_attribute10,
    global_attribute11,
    global_attribute12,
    global_attribute13,
    global_attribute14,
    global_attribute15,
    global_attribute16,
    global_attribute17,
    global_attribute18,
    global_attribute19,
    global_attribute20,
    wf_item_type,
    wf_item_key,
    pcard_id,
    pay_on_code,
    xml_flag,
    xml_send_date,
    xml_change_send_date,
    consigned_consumption_flag,
    cbc_accounting_date,
    change_requested_by,
    shipping_control,
    change_summary,
    vendor_order_num,
    document_creation_method,
    submit_date,
    tax_attribute_update_code,
    comm_rev_num,
    otm_status_code,
    otm_recovery_flag,
    `cancel_reason#1`,
    KCA_OPERATION,
    'N' AS IS_DELETED_FLG,
    CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
    KCA_SEQ_DATE
  FROM bronze_bec_ods_stg.po_releases_all
  WHERE
    kca_operation IN ('INSERT', 'UPDATE')
    AND (PO_RELEASE_ID, kca_seq_id) IN (
      SELECT
        PO_RELEASE_ID,
        MAX(kca_seq_id)
      FROM bronze_bec_ods_stg.po_releases_all
      WHERE
        kca_operation IN ('INSERT', 'UPDATE')
      GROUP BY
        PO_RELEASE_ID
    )
);
/* Soft delete */
UPDATE silver_bec_ods.po_releases_all SET IS_DELETED_FLG = 'N';
UPDATE silver_bec_ods.po_releases_all SET IS_DELETED_FLG = 'Y'
WHERE
  (
    PO_RELEASE_ID
  ) IN (
    SELECT
      PO_RELEASE_ID
    FROM bec_raw_dl_ext.po_releases_all
    WHERE
      (PO_RELEASE_ID, KCA_SEQ_ID) IN (
        SELECT
          PO_RELEASE_ID,
          MAX(KCA_SEQ_ID) AS KCA_SEQ_ID
        FROM bec_raw_dl_ext.po_releases_all
        GROUP BY
          PO_RELEASE_ID
      )
      AND kca_operation = 'DELETE'
  );
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_releases_all';