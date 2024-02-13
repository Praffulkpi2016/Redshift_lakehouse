/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.PO_REQUISITION_LINES_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.PO_REQUISITION_LINES_ALL
(
	requisition_line_id NUMERIC(15,0)   ENCODE az64
	,requisition_header_id NUMERIC(15,0)   ENCODE az64
	,line_num NUMERIC(15,0)   ENCODE az64
	,line_type_id NUMERIC(15,0)   ENCODE az64
	,category_id NUMERIC(15,0)   ENCODE az64
	,item_description VARCHAR(240)   ENCODE lzo
	,unit_meas_lookup_code VARCHAR(25)   ENCODE lzo
	,unit_price NUMERIC(28,10)   ENCODE az64
	,quantity NUMERIC(28,10)   ENCODE az64
	,deliver_to_location_id NUMERIC(15,0)   ENCODE az64
	,to_person_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,source_type_code VARCHAR(25)   ENCODE lzo
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,item_id NUMERIC(15,0)   ENCODE az64
	,item_revision VARCHAR(3)   ENCODE lzo
	,quantity_delivered NUMERIC(28,10)   ENCODE az64
	,suggested_buyer_id NUMERIC(9,0)   ENCODE az64
	,encumbered_flag VARCHAR(1)   ENCODE lzo
	,rfq_required_flag VARCHAR(1)   ENCODE lzo
	,need_by_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,line_location_id NUMERIC(15,0)   ENCODE az64
	,modified_by_agent_flag VARCHAR(1)   ENCODE lzo
	,parent_req_line_id NUMERIC(15,0)   ENCODE az64
	,justification VARCHAR(480)   ENCODE lzo
	,note_to_agent VARCHAR(480)   ENCODE lzo
	,note_to_receiver VARCHAR(480)   ENCODE lzo
	,purchasing_agent_id NUMERIC(9,0)   ENCODE az64
	,document_type_code VARCHAR(25)   ENCODE lzo
	,blanket_po_header_id NUMERIC(15,0)   ENCODE az64
	,blanket_po_line_num NUMERIC(15,0)   ENCODE az64
	,currency_code VARCHAR(15)   ENCODE lzo
	,rate_type VARCHAR(30)   ENCODE lzo
	,rate_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,rate NUMERIC(28,10)   ENCODE az64
	,currency_unit_price NUMERIC(28,10)   ENCODE az64
	,suggested_vendor_name VARCHAR(240)   ENCODE lzo
	,suggested_vendor_location VARCHAR(240)   ENCODE lzo
	,suggested_vendor_contact VARCHAR(80)   ENCODE lzo
	,suggested_vendor_phone VARCHAR(25)   ENCODE lzo
	,suggested_vendor_product_code VARCHAR(25)   ENCODE lzo
	,un_number_id NUMERIC(15,0)   ENCODE az64
	,hazard_class_id NUMERIC(15,0)   ENCODE az64
	,must_use_sugg_vendor_flag VARCHAR(1)   ENCODE lzo
	,reference_num VARCHAR(25)   ENCODE lzo
	,on_rfq_flag VARCHAR(1)   ENCODE lzo
	,urgent_flag VARCHAR(1)   ENCODE lzo
	,cancel_flag VARCHAR(1)   ENCODE lzo
	,source_organization_id NUMERIC(15,0)   ENCODE az64
	,source_subinventory VARCHAR(10)   ENCODE lzo
	,destination_type_code VARCHAR(25)   ENCODE lzo
	,destination_organization_id NUMERIC(15,0)   ENCODE az64
	,destination_subinventory VARCHAR(10)   ENCODE lzo
	,quantity_cancelled NUMERIC(28,10)   ENCODE az64
	,cancel_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cancel_reason VARCHAR(240)   ENCODE lzo
	,closed_code VARCHAR(25)   ENCODE lzo
	,agent_return_note VARCHAR(480)   ENCODE lzo
	,changed_after_research_flag VARCHAR(1)   ENCODE lzo
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,vendor_site_id NUMERIC(15,0)   ENCODE az64
	,vendor_contact_id NUMERIC(15,0)   ENCODE az64
	,research_agent_id NUMERIC(9,0)   ENCODE az64
	,on_line_flag VARCHAR(1)   ENCODE lzo
	,wip_entity_id NUMERIC(15,0)   ENCODE az64
	,wip_line_id NUMERIC(15,0)   ENCODE az64
	,wip_repetitive_schedule_id NUMERIC(15,0)   ENCODE az64
	,wip_operation_seq_num NUMERIC(15,0)   ENCODE az64
	,wip_resource_seq_num NUMERIC(15,0)   ENCODE az64
	,attribute_category VARCHAR(30)   ENCODE lzo
	,destination_context VARCHAR(30)   ENCODE lzo
	,inventory_source_context VARCHAR(30)   ENCODE lzo
	,vendor_source_context VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,bom_resource_id NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,government_context VARCHAR(30)   ENCODE lzo
	,closed_reason VARCHAR(240)   ENCODE lzo
	,closed_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,transaction_reason_code VARCHAR(25)   ENCODE lzo
	,quantity_received NUMERIC(28,10)   ENCODE az64
	,source_req_line_id NUMERIC(15,0)   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,global_attribute1 VARCHAR(150)   ENCODE lzo
	,global_attribute2 VARCHAR(150)   ENCODE lzo
	,global_attribute3 VARCHAR(150)   ENCODE lzo
	,global_attribute4 VARCHAR(150)   ENCODE lzo
	,global_attribute5 VARCHAR(150)   ENCODE lzo
	,global_attribute6 VARCHAR(150)   ENCODE lzo
	,global_attribute7 VARCHAR(150)   ENCODE lzo
	,global_attribute8 VARCHAR(150)   ENCODE lzo
	,global_attribute9 VARCHAR(150)   ENCODE lzo
	,global_attribute10 VARCHAR(150)   ENCODE lzo
	,global_attribute11 VARCHAR(150)   ENCODE lzo
	,global_attribute12 VARCHAR(150)   ENCODE lzo
	,global_attribute13 VARCHAR(150)   ENCODE lzo
	,global_attribute14 VARCHAR(150)   ENCODE lzo
	,global_attribute15 VARCHAR(150)   ENCODE lzo
	,global_attribute16 VARCHAR(150)   ENCODE lzo
	,global_attribute17 VARCHAR(150)   ENCODE lzo
	,global_attribute18 VARCHAR(150)   ENCODE lzo
	,global_attribute19 VARCHAR(150)   ENCODE lzo
	,global_attribute20 VARCHAR(150)   ENCODE lzo
	,global_attribute_category VARCHAR(30)   ENCODE lzo
	,kanban_card_id NUMERIC(15,0)   ENCODE az64
	,catalog_type VARCHAR(30)   ENCODE lzo
	,catalog_source VARCHAR(30)   ENCODE lzo
	,manufacturer_id NUMERIC(15,0)   ENCODE az64
	,manufacturer_name VARCHAR(240)   ENCODE lzo
	,manufacturer_part_number VARCHAR(150)   ENCODE lzo
	,requester_email VARCHAR(240)   ENCODE lzo
	,requester_fax VARCHAR(60)   ENCODE lzo
	,requester_phone VARCHAR(60)   ENCODE lzo
	,unspsc_code VARCHAR(30)   ENCODE lzo
	,other_category_code VARCHAR(30)   ENCODE lzo
	,supplier_duns VARCHAR(60)   ENCODE lzo
	,tax_status_indicator VARCHAR(30)   ENCODE lzo
	,pcard_flag VARCHAR(1)   ENCODE lzo
	,new_supplier_flag VARCHAR(1)   ENCODE lzo
	,auto_receive_flag VARCHAR(1)   ENCODE lzo
	,tax_user_override_flag VARCHAR(1)   ENCODE lzo
	,tax_code_id NUMERIC(15,0)   ENCODE az64
	,note_to_vendor VARCHAR(480)   ENCODE lzo
	,oke_contract_version_id NUMERIC(15,0)   ENCODE az64
	,oke_contract_header_id NUMERIC(15,0)   ENCODE az64
	,item_source_id NUMERIC(15,0)   ENCODE az64
	,supplier_ref_number VARCHAR(150)   ENCODE lzo
	,secondary_unit_of_measure VARCHAR(25)   ENCODE lzo
	,secondary_quantity NUMERIC(28,10)   ENCODE az64
	,preferred_grade VARCHAR(150)   ENCODE lzo
	,secondary_quantity_received NUMERIC(28,10)   ENCODE az64
	,secondary_quantity_cancelled NUMERIC(28,10)   ENCODE az64
	,vmi_flag VARCHAR(1)   ENCODE lzo
	,auction_header_id NUMERIC(15,0)   ENCODE az64
	,auction_display_number VARCHAR(40)   ENCODE lzo
	,auction_line_number NUMERIC(15,0)   ENCODE az64
	,reqs_in_pool_flag VARCHAR(1)   ENCODE lzo
	,bid_number NUMERIC(15,0)   ENCODE az64
	,bid_line_number NUMERIC(15,0)   ENCODE az64
	,noncat_template_id NUMERIC(15,0)   ENCODE az64
	,suggested_vendor_contact_fax VARCHAR(25)   ENCODE lzo
	,suggested_vendor_contact_email VARCHAR(2000)   ENCODE lzo
	,amount NUMERIC(28,10)   ENCODE az64
	,currency_amount NUMERIC(28,10)   ENCODE az64
	,labor_req_line_id NUMERIC(15,0)   ENCODE az64
	,job_id NUMERIC(15,0)   ENCODE az64
	,job_long_description VARCHAR(2000)   ENCODE lzo
	,contractor_status VARCHAR(25)   ENCODE lzo
	,contact_information VARCHAR(240)   ENCODE lzo
	,suggested_supplier_flag VARCHAR(1)   ENCODE lzo
	,candidate_screening_reqd_flag VARCHAR(1)   ENCODE lzo
	,assignment_end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,overtime_allowed_flag VARCHAR(1)   ENCODE lzo
	,contractor_requisition_flag VARCHAR(1)   ENCODE lzo
	,drop_ship_flag VARCHAR(1)   ENCODE lzo
	,candidate_first_name VARCHAR(240)   ENCODE lzo
	,candidate_last_name VARCHAR(240)   ENCODE lzo
	,assignment_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,order_type_lookup_code VARCHAR(25)   ENCODE lzo
	,purchase_basis VARCHAR(30)   ENCODE lzo
	,matching_basis VARCHAR(30)   ENCODE lzo
	,negotiated_by_preparer_flag VARCHAR(1)   ENCODE lzo
	,ship_method VARCHAR(30)   ENCODE lzo
	,estimated_pickup_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,supplier_notified_for_cancel VARCHAR(1)   ENCODE lzo
	,base_unit_price NUMERIC(28,10)   ENCODE az64
	,at_sourcing_flag VARCHAR(1)   ENCODE lzo
	,tax_attribute_update_code VARCHAR(15)   ENCODE lzo
	,tax_name VARCHAR(30)   ENCODE lzo
	,AMENDMENT_RESPONSE_REASON VARCHAR(2000)   ENCODE lzo
	,AMENDMENT_STATUS VARCHAR(3)   ENCODE lzo
	,AMENDMENT_TYPE VARCHAR(30)   ENCODE lzo
	,ASSIGNMENT_NUMBER VARCHAR(30)   ENCODE lzo
	,BUYER_ASSISTANCE_REQUIRED VARCHAR(1)   ENCODE lzo
	,CLM_BASE_LINE_NUM NUMERIC(15,0)   ENCODE az64
	,CLM_EXERCISED_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,CLM_EXTENDED_ITEM_DESCRIPTION VARCHAR(4000)   ENCODE lzo
	,CLM_FUNDED_FLAG VARCHAR(1)   ENCODE lzo
	,CLM_INFO_FLAG VARCHAR(1)   ENCODE lzo
	,CLM_MIPR_OBLIGATION_TYPE VARCHAR(30)   ENCODE lzo
	,CLM_OPTION_EXERCISED VARCHAR(1)   ENCODE lzo
	,CLM_OPTION_FROM_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,CLM_OPTION_INDICATOR VARCHAR(1)   ENCODE lzo
	,CLM_OPTION_NUM NUMERIC(15,0)   ENCODE az64
	,CLM_OPTION_TO_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,CLM_PERIOD_PERF_END_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,CLM_PERIOD_PERF_START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,"CLOSED_REASON#1" VARCHAR(2000)   ENCODE lzo
	,CONFORMED_LINE_ID NUMERIC(15,0)   ENCODE az64
	,CONTRACT_TYPE VARCHAR(100)   ENCODE lzo
	,COST_CONSTRAINT VARCHAR(100)   ENCODE lzo
	,CREATESPOTBUYFLAG VARCHAR(5)   ENCODE lzo
	,FUND_SOURCE_NOT_KNOWN VARCHAR(1)   ENCODE lzo
	,GROUP_LINE_ID NUMERIC(15,0)   ENCODE az64
	,IS_LINE_NUM_DISPLAY_MODIFIED VARCHAR(1)   ENCODE lzo
	,LINE_NUM_DISPLAY VARCHAR(100)   ENCODE lzo
	,LINKED_PO_COUNT NUMERIC(15,0)   ENCODE az64
	,LINKED_TO_FUND VARCHAR(1)   ENCODE lzo
	,NEWSUPP_REGISTRATION_ID NUMERIC(15,0)   ENCODE az64
	,PAR_DRAFT_ID NUMERIC(15,0)   ENCODE az64
	,PAR_LINE_ID NUMERIC(15,0)   ENCODE az64
	,PO_DRAFT_ID NUMERIC(15,0)   ENCODE az64
	,PO_LINE_ID NUMERIC(15,0)   ENCODE az64
	,PUNCHOUT_ITEM_ID NUMERIC(15,0)   ENCODE az64
	,REQ_TEMPLATE_LINE_NUM NUMERIC(15,0)   ENCODE az64
	,REQ_TEMPLATE_NAME VARCHAR(25)   ENCODE lzo
	,SPOT_BUY_FLAG VARCHAR(5)   ENCODE lzo
	,SUGGEST_MULTIPLE_SUPPLIERS VARCHAR(5)   ENCODE lzo
	,"SUGGESTED_VENDOR_CONTACT_FAX#1" VARCHAR(40)   ENCODE lzo
	,"SUGGESTED_VENDOR_CONTACT#1" VARCHAR(150)   ENCODE lzo
	,"SUGGESTED_VENDOR_PHONE#1" VARCHAR(40)   ENCODE lzo
	,TRANSFERRED_TO_OE_FLAG VARCHAR(1)   ENCODE lzo
	,UDA_TEMPLATE_ID NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
		,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.PO_REQUISITION_LINES_ALL (
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
	"closed_reason#1",
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
	"suggested_vendor_contact_fax#1",
	"suggested_vendor_contact#1",
	"suggested_vendor_phone#1",
	transferred_to_oe_flag,
	uda_template_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
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
	"closed_reason#1",
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
	"suggested_vendor_contact_fax#1",
	"suggested_vendor_contact#1",
	"suggested_vendor_phone#1",
	transferred_to_oe_flag,
	uda_template_id,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.PO_REQUISITION_LINES_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'po_requisition_lines_all';
	
commit;