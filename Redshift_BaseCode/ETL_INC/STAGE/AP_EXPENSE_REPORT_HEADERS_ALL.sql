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

truncate table bec_ods_stg.AP_EXPENSE_REPORT_HEADERS_ALL;
	
	insert into	bec_ods_stg.AP_EXPENSE_REPORT_HEADERS_ALL
(
report_header_id,
	employee_id,
	week_end_date,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	vouchno,
	total,
	vendor_id,
	vendor_site_id,
	expense_check_address_flag,
	reference_1,
	reference_2,
	invoice_num,
	expense_report_id,
	accts_pay_code_combination_id,
	set_of_books_id,
	"source",
	purgeable_flag,
	accounting_date,
	maximum_amount_to_apply,
	advance_invoice_to_apply,
	apply_advances_default,
	employee_ccid,
	description,
	reject_code,
	hold_lookup_code,
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
	default_currency_code,
	default_exchange_rate_type,
	default_exchange_rate,
	default_exchange_date,
	last_update_login,
	voucher_num,
	ussgl_transaction_code,
	ussgl_trx_code_context,
	doc_category_code,
	awt_group_id,
	org_id,
	workflow_approved_flag,
	flex_concatenated,
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
	override_approver_id,
	payment_cross_rate_type,
	payment_cross_rate_date,
	payment_cross_rate,
	payment_currency_code,
	core_wf_status_flag,
	prepay_apply_flag,
	prepay_num,
	prepay_dist_num,
	prepay_apply_amount,
	prepay_gl_date,
	bothpay_parent_id,
	shortpay_parent_id,
	paid_on_behalf_employee_id,
	override_approver_name,
	amt_due_ccard_company,
	amt_due_employee,
	default_receipt_currency_code,
	multiple_currencies_flag,
	expense_status_code,
	expense_last_status_date,
	expense_current_approver_id,
	report_filing_number,
	receipts_received_date,
	audit_code,
	report_submitted_date,
	last_audited_by,
	return_reason_code,
	return_instruction,
	receipts_status,
	holding_report_header_id,
	request_id,
	advances_justification,
	IMAGE_RECEIPTS_STATUS,
	IMAGE_RECEIPTS_RECEIVED_DATE,
	MISSING_IMG_JUST,
	APPROVAL_TYPE,
	OVERDUE_REQUEST_ID,
	KCA_OPERATION
	,kca_seq_id
	,kca_seq_date
)

( 
SELECT
report_header_id,
	employee_id,
	week_end_date,
	creation_date,
	created_by,
	last_update_date,
	last_updated_by,
	vouchno,
	total,
	vendor_id,
	vendor_site_id,
	expense_check_address_flag,
	reference_1,
	reference_2,
	invoice_num,
	expense_report_id,
	accts_pay_code_combination_id,
	set_of_books_id,
	"source",
	purgeable_flag,
	accounting_date,
	maximum_amount_to_apply,
	advance_invoice_to_apply,
	apply_advances_default,
	employee_ccid,
	description,
	reject_code,
	hold_lookup_code,
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
	default_currency_code,
	default_exchange_rate_type,
	default_exchange_rate,
	default_exchange_date,
	last_update_login,
	voucher_num,
	ussgl_transaction_code,
	ussgl_trx_code_context,
	doc_category_code,
	awt_group_id,
	org_id,
	workflow_approved_flag,
	flex_concatenated,
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
	override_approver_id,
	payment_cross_rate_type,
	payment_cross_rate_date,
	payment_cross_rate,
	payment_currency_code,
	core_wf_status_flag,
	prepay_apply_flag,
	prepay_num,
	prepay_dist_num,
	prepay_apply_amount,
	prepay_gl_date,
	bothpay_parent_id,
	shortpay_parent_id,
	paid_on_behalf_employee_id,
	override_approver_name,
	amt_due_ccard_company,
	amt_due_employee,
	default_receipt_currency_code,
	multiple_currencies_flag,
	expense_status_code,
	expense_last_status_date,
	expense_current_approver_id,
	report_filing_number,
	receipts_received_date,
	audit_code,
	report_submitted_date,
	last_audited_by,
	return_reason_code,
	return_instruction,
	receipts_status,
	holding_report_header_id,
	request_id,
	advances_justification,
	IMAGE_RECEIPTS_STATUS,
	IMAGE_RECEIPTS_RECEIVED_DATE,
	MISSING_IMG_JUST,
	APPROVAL_TYPE,
	OVERDUE_REQUEST_ID,
	KCA_OPERATION
	,kca_seq_id
	,kca_seq_date
	from
		bec_raw_dl_ext.ap_expense_report_headers_all
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
and (REPORT_HEADER_ID,kca_seq_id) in (select REPORT_HEADER_ID,max(kca_seq_id) 
from bec_raw_dl_ext.ap_expense_report_headers_all 
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
group by REPORT_HEADER_ID)
and 
kca_seq_date > (
select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name = 'ap_expense_report_headers_all')
);
end;