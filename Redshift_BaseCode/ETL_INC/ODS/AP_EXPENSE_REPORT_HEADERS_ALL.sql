/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;


-- Delete Records

delete from bec_ods.ap_expense_report_headers_all
where REPORT_HEADER_ID in (
select stg.REPORT_HEADER_ID from bec_ods.ap_expense_report_headers_all ods, bec_ods_stg.ap_expense_report_headers_all stg
where ods.REPORT_HEADER_ID = stg.REPORT_HEADER_ID and stg.kca_operation IN ('INSERT', 'UPDATE'));

commit;


-- Insert records

insert
	into
	bec_ods.AP_EXPENSE_REPORT_HEADERS_ALL
(REPORT_HEADER_ID,
	EMPLOYEE_ID,
	WEEK_END_DATE,
	CREATION_DATE,
	CREATED_BY,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
	VOUCHNO,
	TOTAL,
	VENDOR_ID,
	VENDOR_SITE_ID,
	EXPENSE_CHECK_ADDRESS_FLAG,
	REFERENCE_1,
	REFERENCE_2,
	INVOICE_NUM,
	EXPENSE_REPORT_ID,
	ACCTS_PAY_CODE_COMBINATION_ID,
	SET_OF_BOOKS_ID,
	"SOURCE",
	PURGEABLE_FLAG,
	ACCOUNTING_DATE,
	MAXIMUM_AMOUNT_TO_APPLY,
	ADVANCE_INVOICE_TO_APPLY,
	APPLY_ADVANCES_DEFAULT,
	EMPLOYEE_CCID,
	DESCRIPTION,
	REJECT_CODE,
	HOLD_LOOKUP_CODE,
	ATTRIBUTE_CATEGORY,
	ATTRIBUTE1,
	ATTRIBUTE2,
	ATTRIBUTE3,
	ATTRIBUTE4,
	ATTRIBUTE5,
	ATTRIBUTE6,
	ATTRIBUTE7,
	ATTRIBUTE8,
	ATTRIBUTE9,
	ATTRIBUTE10,
	ATTRIBUTE11,
	ATTRIBUTE12,
	ATTRIBUTE13,
	ATTRIBUTE14,
	ATTRIBUTE15,
	DEFAULT_CURRENCY_CODE,
	DEFAULT_EXCHANGE_RATE_TYPE,
	DEFAULT_EXCHANGE_RATE,
	DEFAULT_EXCHANGE_DATE,
	LAST_UPDATE_LOGIN,
	VOUCHER_NUM,
	USSGL_TRANSACTION_CODE,
	USSGL_TRX_CODE_CONTEXT,
	DOC_CATEGORY_CODE,
	AWT_GROUP_ID,
	ORG_ID,
	WORKFLOW_APPROVED_FLAG,
	FLEX_CONCATENATED,
	GLOBAL_ATTRIBUTE_CATEGORY,
	GLOBAL_ATTRIBUTE1,
	GLOBAL_ATTRIBUTE2,
	GLOBAL_ATTRIBUTE3,
	GLOBAL_ATTRIBUTE4,
	GLOBAL_ATTRIBUTE5,
	GLOBAL_ATTRIBUTE6,
	GLOBAL_ATTRIBUTE7,
	GLOBAL_ATTRIBUTE8,
	GLOBAL_ATTRIBUTE9,
	GLOBAL_ATTRIBUTE10,
	GLOBAL_ATTRIBUTE11,
	GLOBAL_ATTRIBUTE12,
	GLOBAL_ATTRIBUTE13,
	GLOBAL_ATTRIBUTE14,
	GLOBAL_ATTRIBUTE15,
	GLOBAL_ATTRIBUTE16,
	GLOBAL_ATTRIBUTE17,
	GLOBAL_ATTRIBUTE18,
	GLOBAL_ATTRIBUTE19,
	GLOBAL_ATTRIBUTE20,
	OVERRIDE_APPROVER_ID,
	PAYMENT_CROSS_RATE_TYPE,
	PAYMENT_CROSS_RATE_DATE,
	PAYMENT_CROSS_RATE,
	PAYMENT_CURRENCY_CODE,
	CORE_WF_STATUS_FLAG,
	PREPAY_APPLY_FLAG,
	PREPAY_NUM,
	PREPAY_DIST_NUM,
	PREPAY_APPLY_AMOUNT,
	PREPAY_GL_DATE,
	BOTHPAY_PARENT_ID,
	SHORTPAY_PARENT_ID,
	PAID_ON_BEHALF_EMPLOYEE_ID,
	OVERRIDE_APPROVER_NAME,
	AMT_DUE_CCARD_COMPANY,
	AMT_DUE_EMPLOYEE,
	DEFAULT_RECEIPT_CURRENCY_CODE,
	MULTIPLE_CURRENCIES_FLAG,
	EXPENSE_STATUS_CODE,
	EXPENSE_LAST_STATUS_DATE,
	EXPENSE_CURRENT_APPROVER_ID,
	REPORT_FILING_NUMBER,
	RECEIPTS_RECEIVED_DATE,
	AUDIT_CODE,
	REPORT_SUBMITTED_DATE,
	LAST_AUDITED_BY,
	RETURN_REASON_CODE,
	RETURN_INSTRUCTION,
	RECEIPTS_STATUS,
	HOLDING_REPORT_HEADER_ID,
	REQUEST_ID,
	ADVANCES_JUSTIFICATION,
	IMAGE_RECEIPTS_STATUS,
	IMAGE_RECEIPTS_RECEIVED_DATE,
	MISSING_IMG_JUST,
	APPROVAL_TYPE,
	OVERDUE_REQUEST_ID
	,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date 
	)	
(
select
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
	KCA_OPERATION,
'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from
	bec_ods_stg.ap_expense_report_headers_all
where kca_operation IN ('INSERT','UPDATE') and (REPORT_HEADER_ID,kca_seq_id) in (select REPORT_HEADER_ID,max(kca_seq_id) from bec_ods_stg.ap_expense_report_headers_all 
where kca_operation IN ('INSERT','UPDATE')
group by REPORT_HEADER_ID)
);

commit;

-- Soft delete
update bec_ods.ap_expense_report_headers_all set IS_DELETED_FLG = 'N';
commit;
update bec_ods.ap_expense_report_headers_all set IS_DELETED_FLG = 'Y'
where (REPORT_HEADER_ID)  in
(
select REPORT_HEADER_ID from bec_raw_dl_ext.ap_expense_report_headers_all
where (REPORT_HEADER_ID,KCA_SEQ_ID)
in 
(
select REPORT_HEADER_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ap_expense_report_headers_all
group by REPORT_HEADER_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update
	bec_etl_ctrl.batch_ods_info
set load_type = 'I',
	last_refresh_date = getdate()
where
	ods_table_name = 'ap_expense_report_headers_all';

commit;