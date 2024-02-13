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

delete from bec_ods.AR_CASH_RECEIPTS_ALL
where cash_receipt_id in (
select stg.cash_receipt_id from bec_ods.AR_CASH_RECEIPTS_ALL ods, bec_ods_stg.AR_CASH_RECEIPTS_ALL stg
where ods.cash_receipt_id = stg.cash_receipt_id and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.AR_CASH_RECEIPTS_ALL
       (
    cash_receipt_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	amount,
	set_of_books_id,
	currency_code,
	receivables_trx_id,
	pay_from_customer,
	status,
	"type",
	receipt_number,
	receipt_date,
	misc_payment_source,
	comments,
	distribution_set_id,
	reversal_date,
	reversal_category,
	reversal_reason_code,
	reversal_comments,
	exchange_rate_type,
	exchange_rate,
	exchange_date,
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
	remittance_bank_account_id,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	confirmed_flag,
	customer_bank_account_id,
	customer_site_use_id,
	deposit_date,
	program_application_id,
	program_id,
	program_update_date,
	receipt_method_id,
	request_id,
	selected_for_factoring_flag,
	selected_remittance_batch_id,
	factor_discount_amount,
	ussgl_transaction_code,
	ussgl_transaction_code_context,
	doc_sequence_value,
	doc_sequence_id,
	vat_tax_id,
	reference_type,
	reference_id,
	customer_receipt_reference,
	override_remit_account_flag,
	org_id,
	anticipated_clearing_date,
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
	customer_bank_branch_id,
	issuer_name,
	issue_date,
	issuer_bank_branch_id,
	mrc_exchange_rate_type,
	mrc_exchange_rate,
	mrc_exchange_date,
	payment_server_order_num,
	approval_code,
	address_verification_code,
	tax_rate,
	actual_value_date,
	postmark_date,
	application_notes,
	unique_reference,
	promise_source,
	rec_version_number,
	cc_error_code,
	cc_error_text,
	cc_error_flag,
	remit_bank_acct_use_id,
	old_customer_bank_branch_id,
	old_issuer_bank_branch_id,
	legal_entity_id,
	payment_trxn_extension_id,
	ax_accounted_flag,
	old_customer_bank_account_id,
	cash_appln_owner_id,
	work_item_assignment_date,
	work_item_review_date,
	work_item_status_code,
	work_item_review_note,
	prev_pay_from_customer,
	prev_customer_site_use_id,
	work_item_exception_reason, 
	AUTOMATCH_SET_ID,
	AUTOAPPLY_FLAG,
	SEQ_TYPE_LAST,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
(
	SELECT
	cash_receipt_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	amount,
	set_of_books_id,
	currency_code,
	receivables_trx_id,
	pay_from_customer,
	status,
	"type",
	receipt_number,
	receipt_date,
	misc_payment_source,
	comments,
	distribution_set_id,
	reversal_date,
	reversal_category,
	reversal_reason_code,
	reversal_comments,
	exchange_rate_type,
	exchange_rate,
	exchange_date,
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
	remittance_bank_account_id,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	confirmed_flag,
	customer_bank_account_id,
	customer_site_use_id,
	deposit_date,
	program_application_id,
	program_id,
	program_update_date,
	receipt_method_id,
	request_id,
	selected_for_factoring_flag,
	selected_remittance_batch_id,
	factor_discount_amount,
	ussgl_transaction_code,
	ussgl_transaction_code_context,
	doc_sequence_value,
	doc_sequence_id,
	vat_tax_id,
	reference_type,
	reference_id,
	customer_receipt_reference,
	override_remit_account_flag,
	org_id,
	anticipated_clearing_date,
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
	customer_bank_branch_id,
	issuer_name,
	issue_date,
	issuer_bank_branch_id,
	mrc_exchange_rate_type,
	mrc_exchange_rate,
	mrc_exchange_date,
	payment_server_order_num,
	approval_code,
	address_verification_code,
	tax_rate,
	actual_value_date,
	postmark_date,
	application_notes,
	unique_reference,
	promise_source,
	rec_version_number,
	cc_error_code,
	cc_error_text,
	cc_error_flag,
	remit_bank_acct_use_id,
	old_customer_bank_branch_id,
	old_issuer_bank_branch_id,
	legal_entity_id,
	payment_trxn_extension_id,
	ax_accounted_flag,
	old_customer_bank_account_id,
	cash_appln_owner_id,
	work_item_assignment_date,
	work_item_review_date,
	work_item_status_code,
	work_item_review_note,
	prev_pay_from_customer,
	prev_customer_site_use_id,
	work_item_exception_reason, 
	AUTOMATCH_SET_ID,
	AUTOAPPLY_FLAG,
	SEQ_TYPE_LAST,
	kca_operation,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.AR_CASH_RECEIPTS_ALL
	where kca_operation IN ('INSERT','UPDATE') 
	and (cash_receipt_id,kca_seq_id) in 
	(select cash_receipt_id,max(kca_seq_id) from bec_ods_stg.AR_CASH_RECEIPTS_ALL 
     where kca_operation IN ('INSERT','UPDATE')
     group by cash_receipt_id)
);

commit;

-- Soft delete
update bec_ods.AR_CASH_RECEIPTS_ALL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.AR_CASH_RECEIPTS_ALL set IS_DELETED_FLG = 'Y'
where (cash_receipt_id )  in
(
select cash_receipt_id  from bec_raw_dl_ext.AR_CASH_RECEIPTS_ALL
where (cash_receipt_id ,KCA_SEQ_ID)
in 
(
select cash_receipt_id ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.AR_CASH_RECEIPTS_ALL
group by cash_receipt_id 
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'ar_cash_receipts_all';

commit;