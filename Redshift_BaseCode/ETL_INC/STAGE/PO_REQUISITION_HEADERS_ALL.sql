/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate table bec_ods_stg.PO_REQUISITION_HEADERS_ALL;

insert into	bec_ods_stg.PO_REQUISITION_HEADERS_ALL
   (requisition_header_id,
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
	--igt_document_flag,
	notify_requester_flag,
	par_draft_id,
	par_flag,
	revision_num,
	suggested_award_no,
	uda_template_date,
	uda_template_id,
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
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
	--igt_document_flag,
	notify_requester_flag,
	par_draft_id,
	par_flag,
	revision_num,
	suggested_award_no,
	uda_template_date,
	uda_template_id,
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE
	from bec_raw_dl_ext.PO_REQUISITION_HEADERS_ALL
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (REQUISITION_HEADER_ID,kca_seq_id) in 
	(select REQUISITION_HEADER_ID,max(kca_seq_id) from bec_raw_dl_ext.PO_REQUISITION_HEADERS_ALL 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by REQUISITION_HEADER_ID)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'po_requisition_headers_all')
            )	
);
end;