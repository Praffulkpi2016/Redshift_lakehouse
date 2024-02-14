TRUNCATE table bronze_bec_ods_stg.PO_REQUISITION_LINES_ALL;
INSERT INTO bronze_bec_ods_stg.PO_REQUISITION_LINES_ALL (
  requisition_line_id,
  requisition_header_id,
  line_num,
  line_type_id,
  category_id,
  item_description,
  unit_meas_lookup_code,
  unit_price,
  quantity,
  deliver_to_location_id,
  to_person_id,
  last_update_date,
  last_updated_by,
  source_type_code,
  last_update_login,
  creation_date,
  created_by,
  item_id,
  item_revision,
  quantity_delivered,
  suggested_buyer_id,
  encumbered_flag,
  rfq_required_flag,
  need_by_date,
  line_location_id,
  modified_by_agent_flag,
  parent_req_line_id,
  justification,
  note_to_agent,
  note_to_receiver,
  purchasing_agent_id,
  document_type_code,
  blanket_po_header_id,
  blanket_po_line_num,
  currency_code,
  rate_type,
  rate_date,
  rate,
  currency_unit_price,
  suggested_vendor_name,
  suggested_vendor_location,
  suggested_vendor_contact,
  suggested_vendor_phone,
  suggested_vendor_product_code,
  un_number_id,
  hazard_class_id,
  must_use_sugg_vendor_flag,
  reference_num,
  on_rfq_flag,
  urgent_flag,
  cancel_flag,
  source_organization_id,
  source_subinventory,
  destination_type_code,
  destination_organization_id,
  destination_subinventory,
  quantity_cancelled,
  cancel_date,
  cancel_reason,
  closed_code,
  agent_return_note,
  changed_after_research_flag,
  vendor_id,
  vendor_site_id,
  vendor_contact_id,
  research_agent_id,
  on_line_flag,
  wip_entity_id,
  wip_line_id,
  wip_repetitive_schedule_id,
  wip_operation_seq_num,
  wip_resource_seq_num,
  attribute_category,
  destination_context,
  inventory_source_context,
  vendor_source_context,
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
  bom_resource_id,
  request_id,
  program_application_id,
  program_id,
  program_update_date,
  ussgl_transaction_code,
  government_context,
  closed_reason,
  closed_date,
  transaction_reason_code,
  quantity_received,
  source_req_line_id,
  org_id,
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
  global_attribute_category,
  kanban_card_id,
  catalog_type,
  catalog_source,
  manufacturer_id,
  manufacturer_name,
  manufacturer_part_number,
  requester_email,
  requester_fax,
  requester_phone,
  unspsc_code,
  other_category_code,
  supplier_duns,
  tax_status_indicator,
  pcard_flag,
  new_supplier_flag,
  auto_receive_flag,
  tax_user_override_flag,
  tax_code_id,
  note_to_vendor,
  oke_contract_version_id,
  oke_contract_header_id,
  item_source_id,
  supplier_ref_number,
  secondary_unit_of_measure,
  secondary_quantity,
  preferred_grade,
  secondary_quantity_received,
  secondary_quantity_cancelled,
  vmi_flag,
  auction_header_id,
  auction_display_number,
  auction_line_number,
  reqs_in_pool_flag,
  bid_number,
  bid_line_number,
  noncat_template_id,
  suggested_vendor_contact_fax,
  suggested_vendor_contact_email,
  amount,
  currency_amount,
  labor_req_line_id,
  job_id,
  job_long_description,
  contractor_status,
  contact_information,
  suggested_supplier_flag,
  candidate_screening_reqd_flag,
  assignment_end_date,
  overtime_allowed_flag,
  contractor_requisition_flag,
  drop_ship_flag,
  candidate_first_name,
  candidate_last_name,
  assignment_start_date,
  order_type_lookup_code,
  purchase_basis,
  matching_basis,
  negotiated_by_preparer_flag,
  ship_method,
  estimated_pickup_date,
  supplier_notified_for_cancel,
  base_unit_price,
  at_sourcing_flag,
  tax_attribute_update_code,
  amendment_response_reason,
  amendment_status,
  amendment_type,
  assignment_number,
  buyer_assistance_required,
  clm_base_line_num,
  clm_exercised_date,
  clm_extended_item_description,
  clm_funded_flag,
  clm_info_flag,
  clm_mipr_obligation_type,
  clm_option_exercised,
  clm_option_from_date,
  clm_option_indicator,
  clm_option_num,
  clm_option_to_date,
  clm_period_perf_end_date,
  clm_period_perf_start_date,
  `closed_reason#1`,
  conformed_line_id,
  contract_type,
  cost_constraint,
  createspotbuyflag,
  fund_source_not_known,
  group_line_id,
  is_line_num_display_modified,
  line_num_display,
  linked_po_count,
  linked_to_fund,
  newsupp_registration_id,
  par_draft_id,
  par_line_id,
  po_draft_id,
  po_line_id,
  punchout_item_id,
  req_template_line_num,
  req_template_name,
  spot_buy_flag,
  suggest_multiple_suppliers,
  `suggested_vendor_contact_fax#1`,
  `suggested_vendor_contact#1`,
  `suggested_vendor_phone#1`,
  transferred_to_oe_flag,
  uda_template_id,
  tax_name,
  kca_operation,
  kca_seq_id,
  KCA_SEQ_DATE
)
(
  SELECT
    requisition_line_id,
    requisition_header_id,
    line_num,
    line_type_id,
    category_id,
    item_description,
    unit_meas_lookup_code,
    unit_price,
    quantity,
    deliver_to_location_id,
    to_person_id,
    last_update_date,
    last_updated_by,
    source_type_code,
    last_update_login,
    creation_date,
    created_by,
    item_id,
    item_revision,
    quantity_delivered,
    suggested_buyer_id,
    encumbered_flag,
    rfq_required_flag,
    need_by_date,
    line_location_id,
    modified_by_agent_flag,
    parent_req_line_id,
    justification,
    note_to_agent,
    note_to_receiver,
    purchasing_agent_id,
    document_type_code,
    blanket_po_header_id,
    blanket_po_line_num,
    currency_code,
    rate_type,
    rate_date,
    rate,
    currency_unit_price,
    suggested_vendor_name,
    suggested_vendor_location,
    suggested_vendor_contact,
    suggested_vendor_phone,
    suggested_vendor_product_code,
    un_number_id,
    hazard_class_id,
    must_use_sugg_vendor_flag,
    reference_num,
    on_rfq_flag,
    urgent_flag,
    cancel_flag,
    source_organization_id,
    source_subinventory,
    destination_type_code,
    destination_organization_id,
    destination_subinventory,
    quantity_cancelled,
    cancel_date,
    cancel_reason,
    closed_code,
    agent_return_note,
    changed_after_research_flag,
    vendor_id,
    vendor_site_id,
    vendor_contact_id,
    research_agent_id,
    on_line_flag,
    wip_entity_id,
    wip_line_id,
    wip_repetitive_schedule_id,
    wip_operation_seq_num,
    wip_resource_seq_num,
    attribute_category,
    destination_context,
    inventory_source_context,
    vendor_source_context,
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
    bom_resource_id,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    ussgl_transaction_code,
    government_context,
    closed_reason,
    closed_date,
    transaction_reason_code,
    quantity_received,
    source_req_line_id,
    org_id,
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
    global_attribute_category,
    kanban_card_id,
    catalog_type,
    catalog_source,
    manufacturer_id,
    manufacturer_name,
    manufacturer_part_number,
    requester_email,
    requester_fax,
    requester_phone,
    unspsc_code,
    other_category_code,
    supplier_duns,
    tax_status_indicator,
    pcard_flag,
    new_supplier_flag,
    auto_receive_flag,
    tax_user_override_flag,
    tax_code_id,
    note_to_vendor,
    oke_contract_version_id,
    oke_contract_header_id,
    item_source_id,
    supplier_ref_number,
    secondary_unit_of_measure,
    secondary_quantity,
    preferred_grade,
    secondary_quantity_received,
    secondary_quantity_cancelled,
    vmi_flag,
    auction_header_id,
    auction_display_number,
    auction_line_number,
    reqs_in_pool_flag,
    bid_number,
    bid_line_number,
    noncat_template_id,
    suggested_vendor_contact_fax,
    suggested_vendor_contact_email,
    amount,
    currency_amount,
    labor_req_line_id,
    job_id,
    job_long_description,
    contractor_status,
    contact_information,
    suggested_supplier_flag,
    candidate_screening_reqd_flag,
    assignment_end_date,
    overtime_allowed_flag,
    contractor_requisition_flag,
    drop_ship_flag,
    candidate_first_name,
    candidate_last_name,
    assignment_start_date,
    order_type_lookup_code,
    purchase_basis,
    matching_basis,
    negotiated_by_preparer_flag,
    ship_method,
    estimated_pickup_date,
    supplier_notified_for_cancel,
    base_unit_price,
    at_sourcing_flag,
    tax_attribute_update_code,
    amendment_response_reason,
    amendment_status,
    amendment_type,
    assignment_number,
    buyer_assistance_required,
    clm_base_line_num,
    clm_exercised_date,
    clm_extended_item_description,
    clm_funded_flag,
    clm_info_flag,
    clm_mipr_obligation_type,
    clm_option_exercised,
    clm_option_from_date,
    clm_option_indicator,
    clm_option_num,
    clm_option_to_date,
    clm_period_perf_end_date,
    clm_period_perf_start_date,
    `closed_reason#1`,
    conformed_line_id,
    contract_type,
    cost_constraint,
    createspotbuyflag,
    fund_source_not_known,
    group_line_id,
    is_line_num_display_modified,
    line_num_display,
    linked_po_count,
    linked_to_fund,
    newsupp_registration_id,
    par_draft_id,
    par_line_id,
    po_draft_id,
    po_line_id,
    punchout_item_id,
    req_template_line_num,
    req_template_name,
    spot_buy_flag,
    suggest_multiple_suppliers,
    `suggested_vendor_contact_fax#1`,
    `suggested_vendor_contact#1`,
    `suggested_vendor_phone#1`,
    transferred_to_oe_flag,
    uda_template_id,
    tax_name,
    kca_operation,
    kca_seq_id,
    KCA_SEQ_DATE
  FROM bec_raw_dl_ext.PO_REQUISITION_LINES_ALL
  WHERE
    kca_operation <> 'DELETE'
    AND COALESCE(kca_seq_id, '') <> ''
    AND (REQUISITION_LINE_ID, kca_seq_id) IN (
      SELECT
        REQUISITION_LINE_ID,
        MAX(kca_seq_id)
      FROM bec_raw_dl_ext.PO_REQUISITION_LINES_ALL
      WHERE
        kca_operation <> 'DELETE' AND COALESCE(kca_seq_id, '') <> ''
      GROUP BY
        REQUISITION_LINE_ID
    )
    AND (
      KCA_SEQ_DATE > (
        SELECT
          (
            executebegints - prune_days
          )
        FROM bec_etl_ctrl.batch_ods_info
        WHERE
          ods_table_name = 'po_requisition_lines_all'
      )
    )
);