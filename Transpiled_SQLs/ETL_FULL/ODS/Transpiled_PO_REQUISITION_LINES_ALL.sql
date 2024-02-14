DROP TABLE IF EXISTS silver_bec_ods.PO_REQUISITION_LINES_ALL;
CREATE TABLE IF NOT EXISTS silver_bec_ods.PO_REQUISITION_LINES_ALL (
  requisition_line_id DECIMAL(15, 0),
  requisition_header_id DECIMAL(15, 0),
  line_num DECIMAL(15, 0),
  line_type_id DECIMAL(15, 0),
  category_id DECIMAL(15, 0),
  item_description STRING,
  unit_meas_lookup_code STRING,
  unit_price DECIMAL(28, 10),
  quantity DECIMAL(28, 10),
  deliver_to_location_id DECIMAL(15, 0),
  to_person_id DECIMAL(15, 0),
  last_update_date TIMESTAMP,
  last_updated_by DECIMAL(15, 0),
  source_type_code STRING,
  last_update_login DECIMAL(15, 0),
  creation_date TIMESTAMP,
  created_by DECIMAL(15, 0),
  item_id DECIMAL(15, 0),
  item_revision STRING,
  quantity_delivered DECIMAL(28, 10),
  suggested_buyer_id DECIMAL(9, 0),
  encumbered_flag STRING,
  rfq_required_flag STRING,
  need_by_date TIMESTAMP,
  line_location_id DECIMAL(15, 0),
  modified_by_agent_flag STRING,
  parent_req_line_id DECIMAL(15, 0),
  justification STRING,
  note_to_agent STRING,
  note_to_receiver STRING,
  purchasing_agent_id DECIMAL(9, 0),
  document_type_code STRING,
  blanket_po_header_id DECIMAL(15, 0),
  blanket_po_line_num DECIMAL(15, 0),
  currency_code STRING,
  rate_type STRING,
  rate_date TIMESTAMP,
  rate DECIMAL(28, 10),
  currency_unit_price DECIMAL(28, 10),
  suggested_vendor_name STRING,
  suggested_vendor_location STRING,
  suggested_vendor_contact STRING,
  suggested_vendor_phone STRING,
  suggested_vendor_product_code STRING,
  un_number_id DECIMAL(15, 0),
  hazard_class_id DECIMAL(15, 0),
  must_use_sugg_vendor_flag STRING,
  reference_num STRING,
  on_rfq_flag STRING,
  urgent_flag STRING,
  cancel_flag STRING,
  source_organization_id DECIMAL(15, 0),
  source_subinventory STRING,
  destination_type_code STRING,
  destination_organization_id DECIMAL(15, 0),
  destination_subinventory STRING,
  quantity_cancelled DECIMAL(28, 10),
  cancel_date TIMESTAMP,
  cancel_reason STRING,
  closed_code STRING,
  agent_return_note STRING,
  changed_after_research_flag STRING,
  vendor_id DECIMAL(15, 0),
  vendor_site_id DECIMAL(15, 0),
  vendor_contact_id DECIMAL(15, 0),
  research_agent_id DECIMAL(9, 0),
  on_line_flag STRING,
  wip_entity_id DECIMAL(15, 0),
  wip_line_id DECIMAL(15, 0),
  wip_repetitive_schedule_id DECIMAL(15, 0),
  wip_operation_seq_num DECIMAL(15, 0),
  wip_resource_seq_num DECIMAL(15, 0),
  attribute_category STRING,
  destination_context STRING,
  inventory_source_context STRING,
  vendor_source_context STRING,
  attribute1 STRING,
  attribute2 STRING,
  attribute3 STRING,
  attribute4 STRING,
  attribute5 STRING,
  attribute6 STRING,
  attribute7 STRING,
  attribute8 STRING,
  attribute9 STRING,
  attribute10 STRING,
  attribute11 STRING,
  attribute12 STRING,
  attribute13 STRING,
  attribute14 STRING,
  attribute15 STRING,
  bom_resource_id DECIMAL(15, 0),
  request_id DECIMAL(15, 0),
  program_application_id DECIMAL(15, 0),
  program_id DECIMAL(15, 0),
  program_update_date TIMESTAMP,
  ussgl_transaction_code STRING,
  government_context STRING,
  closed_reason STRING,
  closed_date TIMESTAMP,
  transaction_reason_code STRING,
  quantity_received DECIMAL(28, 10),
  source_req_line_id DECIMAL(15, 0),
  org_id DECIMAL(15, 0),
  global_attribute1 STRING,
  global_attribute2 STRING,
  global_attribute3 STRING,
  global_attribute4 STRING,
  global_attribute5 STRING,
  global_attribute6 STRING,
  global_attribute7 STRING,
  global_attribute8 STRING,
  global_attribute9 STRING,
  global_attribute10 STRING,
  global_attribute11 STRING,
  global_attribute12 STRING,
  global_attribute13 STRING,
  global_attribute14 STRING,
  global_attribute15 STRING,
  global_attribute16 STRING,
  global_attribute17 STRING,
  global_attribute18 STRING,
  global_attribute19 STRING,
  global_attribute20 STRING,
  global_attribute_category STRING,
  kanban_card_id DECIMAL(15, 0),
  catalog_type STRING,
  catalog_source STRING,
  manufacturer_id DECIMAL(15, 0),
  manufacturer_name STRING,
  manufacturer_part_number STRING,
  requester_email STRING,
  requester_fax STRING,
  requester_phone STRING,
  unspsc_code STRING,
  other_category_code STRING,
  supplier_duns STRING,
  tax_status_indicator STRING,
  pcard_flag STRING,
  new_supplier_flag STRING,
  auto_receive_flag STRING,
  tax_user_override_flag STRING,
  tax_code_id DECIMAL(15, 0),
  note_to_vendor STRING,
  oke_contract_version_id DECIMAL(15, 0),
  oke_contract_header_id DECIMAL(15, 0),
  item_source_id DECIMAL(15, 0),
  supplier_ref_number STRING,
  secondary_unit_of_measure STRING,
  secondary_quantity DECIMAL(28, 10),
  preferred_grade STRING,
  secondary_quantity_received DECIMAL(28, 10),
  secondary_quantity_cancelled DECIMAL(28, 10),
  vmi_flag STRING,
  auction_header_id DECIMAL(15, 0),
  auction_display_number STRING,
  auction_line_number DECIMAL(15, 0),
  reqs_in_pool_flag STRING,
  bid_number DECIMAL(15, 0),
  bid_line_number DECIMAL(15, 0),
  noncat_template_id DECIMAL(15, 0),
  suggested_vendor_contact_fax STRING,
  suggested_vendor_contact_email STRING,
  amount DECIMAL(28, 10),
  currency_amount DECIMAL(28, 10),
  labor_req_line_id DECIMAL(15, 0),
  job_id DECIMAL(15, 0),
  job_long_description STRING,
  contractor_status STRING,
  contact_information STRING,
  suggested_supplier_flag STRING,
  candidate_screening_reqd_flag STRING,
  assignment_end_date TIMESTAMP,
  overtime_allowed_flag STRING,
  contractor_requisition_flag STRING,
  drop_ship_flag STRING,
  candidate_first_name STRING,
  candidate_last_name STRING,
  assignment_start_date TIMESTAMP,
  order_type_lookup_code STRING,
  purchase_basis STRING,
  matching_basis STRING,
  negotiated_by_preparer_flag STRING,
  ship_method STRING,
  estimated_pickup_date TIMESTAMP,
  supplier_notified_for_cancel STRING,
  base_unit_price DECIMAL(28, 10),
  at_sourcing_flag STRING,
  tax_attribute_update_code STRING,
  tax_name STRING,
  AMENDMENT_RESPONSE_REASON STRING,
  AMENDMENT_STATUS STRING,
  AMENDMENT_TYPE STRING,
  ASSIGNMENT_NUMBER STRING,
  BUYER_ASSISTANCE_REQUIRED STRING,
  CLM_BASE_LINE_NUM DECIMAL(15, 0),
  CLM_EXERCISED_DATE TIMESTAMP,
  CLM_EXTENDED_ITEM_DESCRIPTION STRING,
  CLM_FUNDED_FLAG STRING,
  CLM_INFO_FLAG STRING,
  CLM_MIPR_OBLIGATION_TYPE STRING,
  CLM_OPTION_EXERCISED STRING,
  CLM_OPTION_FROM_DATE TIMESTAMP,
  CLM_OPTION_INDICATOR STRING,
  CLM_OPTION_NUM DECIMAL(15, 0),
  CLM_OPTION_TO_DATE TIMESTAMP,
  CLM_PERIOD_PERF_END_DATE TIMESTAMP,
  CLM_PERIOD_PERF_START_DATE TIMESTAMP,
  `CLOSED_REASON#1` STRING,
  CONFORMED_LINE_ID DECIMAL(15, 0),
  CONTRACT_TYPE STRING,
  COST_CONSTRAINT STRING,
  CREATESPOTBUYFLAG STRING,
  FUND_SOURCE_NOT_KNOWN STRING,
  GROUP_LINE_ID DECIMAL(15, 0),
  IS_LINE_NUM_DISPLAY_MODIFIED STRING,
  LINE_NUM_DISPLAY STRING,
  LINKED_PO_COUNT DECIMAL(15, 0),
  LINKED_TO_FUND STRING,
  NEWSUPP_REGISTRATION_ID DECIMAL(15, 0),
  PAR_DRAFT_ID DECIMAL(15, 0),
  PAR_LINE_ID DECIMAL(15, 0),
  PO_DRAFT_ID DECIMAL(15, 0),
  PO_LINE_ID DECIMAL(15, 0),
  PUNCHOUT_ITEM_ID DECIMAL(15, 0),
  REQ_TEMPLATE_LINE_NUM DECIMAL(15, 0),
  REQ_TEMPLATE_NAME STRING,
  SPOT_BUY_FLAG STRING,
  SUGGEST_MULTIPLE_SUPPLIERS STRING,
  `SUGGESTED_VENDOR_CONTACT_FAX#1` STRING,
  `SUGGESTED_VENDOR_CONTACT#1` STRING,
  `SUGGESTED_VENDOR_PHONE#1` STRING,
  TRANSFERRED_TO_OE_FLAG STRING,
  UDA_TEMPLATE_ID DECIMAL(15, 0),
  kca_operation STRING,
  is_deleted_flg STRING,
  kca_seq_id DECIMAL(36, 0),
  kca_seq_date TIMESTAMP
);
INSERT INTO silver_bec_ods.PO_REQUISITION_LINES_ALL (
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
  tax_name,
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
  kca_operation,
  is_deleted_flg,
  kca_seq_id,
  kca_seq_date
)
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
  tax_name,
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
  kca_operation,
  'N' AS IS_DELETED_FLG,
  CAST(NULLIF(KCA_SEQ_ID, '') AS DECIMAL(36, 0)) AS KCA_SEQ_ID,
  kca_seq_date
FROM bronze_bec_ods_stg.PO_REQUISITION_LINES_ALL;
UPDATE bec_etl_ctrl.batch_ods_info SET load_type = 'I', last_refresh_date = CURRENT_TIMESTAMP()
WHERE
  ods_table_name = 'po_requisition_lines_all';