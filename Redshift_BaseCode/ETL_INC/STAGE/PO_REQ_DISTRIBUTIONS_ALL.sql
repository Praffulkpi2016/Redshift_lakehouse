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

truncate table bec_ods_stg.PO_REQ_DISTRIBUTIONS_ALL;

insert into	bec_ods_stg.PO_REQ_DISTRIBUTIONS_ALL
   (distribution_id,
	last_update_date,
	last_updated_by,
	requisition_line_id,
	set_of_books_id,
	code_combination_id,
	req_line_quantity,
	last_update_login,
	creation_date,
	created_by,
	encumbered_flag,
	gl_encumbered_date,
	gl_encumbered_period_name,
	gl_cancelled_date,
	failed_funds_lookup_code,
	encumbered_amount,
	budget_account_id,
	accrual_account_id,
	variance_account_id,
	prevent_encumbrance_flag,
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
	ussgl_transaction_code,
	government_context,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	project_id,
	task_id,
	expenditure_type,
	project_accounting_context,
	expenditure_organization_id,
	gl_closed_date,
	source_req_distribution_id,
	distribution_num,
	project_related_flag,
	expenditure_item_date,
	org_id,
	allocation_type,
	allocation_value,
	award_id,
	end_item_unit_number,
	recoverable_tax,
	nonrecoverable_tax,
	recovery_rate,
	tax_recovery_override_flag,
	oke_contract_line_id,
	oke_contract_deliverable_id,
	req_line_amount,
	req_line_currency_amount,
	req_award_id,
	event_id,
	acrn,
	amendment_status,
	amendment_type,
	amount_funded,
	change_in_funded_value,
	clm_agency_acct_identifier,
	clm_defence_funding,
	clm_fms_case_number,
	clm_misc_loa,
	conformed_dist_id,
	funded_value,
	funds_liquidated,
	info_line_id,
	par_distribution_id,
	par_draft_id,
	partial_funded_flag,
	quantity_funded,
	uda_template_id,
	unencumbered_amount,
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE)
(
	select
		distribution_id,
	last_update_date,
	last_updated_by,
	requisition_line_id,
	set_of_books_id,
	code_combination_id,
	req_line_quantity,
	last_update_login,
	creation_date,
	created_by,
	encumbered_flag,
	gl_encumbered_date,
	gl_encumbered_period_name,
	gl_cancelled_date,
	failed_funds_lookup_code,
	encumbered_amount,
	budget_account_id,
	accrual_account_id,
	variance_account_id,
	prevent_encumbrance_flag,
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
	ussgl_transaction_code,
	government_context,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	project_id,
	task_id,
	expenditure_type,
	project_accounting_context,
	expenditure_organization_id,
	gl_closed_date,
	source_req_distribution_id,
	distribution_num,
	project_related_flag,
	expenditure_item_date,
	org_id,
	allocation_type,
	allocation_value,
	award_id,
	end_item_unit_number,
	recoverable_tax,
	nonrecoverable_tax,
	recovery_rate,
	tax_recovery_override_flag,
	oke_contract_line_id,
	oke_contract_deliverable_id,
	req_line_amount,
	req_line_currency_amount,
	req_award_id,
	event_id,
	acrn,
	amendment_status,
	amendment_type,
	amount_funded,
	change_in_funded_value,
	clm_agency_acct_identifier,
	clm_defence_funding,
	clm_fms_case_number,
	clm_misc_loa,
	conformed_dist_id,
	funded_value,
	funds_liquidated,
	info_line_id,
	par_distribution_id,
	par_draft_id,
	partial_funded_flag,
	quantity_funded,
	uda_template_id,
	unencumbered_amount,
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE
	from bec_raw_dl_ext.PO_REQ_DISTRIBUTIONS_ALL
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != '' 
	and (DISTRIBUTION_ID,kca_seq_id) in 
	(select DISTRIBUTION_ID,max(kca_seq_id) from bec_raw_dl_ext.PO_REQ_DISTRIBUTIONS_ALL 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'') != ''
     group by DISTRIBUTION_ID)
        and	(KCA_SEQ_DATE > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'po_req_distributions_all')

            )
);
end;