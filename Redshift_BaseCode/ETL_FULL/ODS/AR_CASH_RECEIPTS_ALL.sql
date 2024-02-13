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

DROP TABLE if exists bec_ods.AR_CASH_RECEIPTS_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.ar_cash_receipts_all
(
	cash_receipt_id NUMERIC(15,0)   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,amount NUMERIC(28,10)   ENCODE az64
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,currency_code VARCHAR(15)   ENCODE lzo
	,receivables_trx_id NUMERIC(15,0)   ENCODE az64
	,pay_from_customer NUMERIC(15,0)   ENCODE az64
	,status VARCHAR(30)   ENCODE lzo
	,"type" VARCHAR(20)   ENCODE lzo
	,receipt_number VARCHAR(30)   ENCODE lzo
	,receipt_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,misc_payment_source VARCHAR(30)   ENCODE lzo
	,comments VARCHAR(2000)   ENCODE lzo
	,distribution_set_id NUMERIC(15,0)   ENCODE az64
	,reversal_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,reversal_category VARCHAR(20)   ENCODE lzo
	,reversal_reason_code VARCHAR(30)   ENCODE lzo
	,reversal_comments VARCHAR(240)   ENCODE lzo
	,exchange_rate_type VARCHAR(30)   ENCODE lzo
	,exchange_rate NUMERIC(28,10)   ENCODE az64
	,exchange_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attribute_category VARCHAR(30)   ENCODE lzo
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
	,remittance_bank_account_id NUMERIC(15,0)   ENCODE az64
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,confirmed_flag VARCHAR(1)   ENCODE lzo
	,customer_bank_account_id NUMERIC(15,0)   ENCODE az64
	,customer_site_use_id NUMERIC(15,0)   ENCODE az64
	,deposit_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,receipt_method_id NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,selected_for_factoring_flag VARCHAR(1)   ENCODE lzo
	,selected_remittance_batch_id NUMERIC(15,0)   ENCODE az64
	,factor_discount_amount NUMERIC(28,10)   ENCODE az64
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,ussgl_transaction_code_context VARCHAR(30)   ENCODE lzo
	,doc_sequence_value NUMERIC(15,0)   ENCODE az64
	,doc_sequence_id NUMERIC(15,0)   ENCODE az64
	,vat_tax_id NUMERIC(15,0)   ENCODE az64
	,reference_type VARCHAR(30)   ENCODE lzo
	,reference_id NUMERIC(15,0)   ENCODE az64
	,customer_receipt_reference VARCHAR(30)   ENCODE lzo
	,override_remit_account_flag VARCHAR(1)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,anticipated_clearing_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,customer_bank_branch_id NUMERIC(15,0)   ENCODE az64
	,issuer_name VARCHAR(50)   ENCODE lzo
	,issue_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,issuer_bank_branch_id NUMERIC(15,0)   ENCODE az64
	,mrc_exchange_rate_type VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_rate VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_date VARCHAR(2000)   ENCODE lzo
	,payment_server_order_num VARCHAR(80)   ENCODE lzo
	,approval_code VARCHAR(80)   ENCODE lzo
	,address_verification_code VARCHAR(80)   ENCODE lzo
	,tax_rate NUMERIC(28,10)   ENCODE az64
	,actual_value_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,postmark_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,application_notes VARCHAR(2000)   ENCODE lzo
	,unique_reference VARCHAR(32)   ENCODE lzo
	,promise_source VARCHAR(30)   ENCODE lzo
	,rec_version_number NUMERIC(15,0)   ENCODE az64
	,cc_error_code VARCHAR(80)   ENCODE lzo
	,cc_error_text VARCHAR(255)   ENCODE lzo
	,cc_error_flag VARCHAR(1)   ENCODE lzo
	,remit_bank_acct_use_id NUMERIC(15,0)   ENCODE az64
	,old_customer_bank_branch_id NUMERIC(15,0)   ENCODE az64
	,old_issuer_bank_branch_id NUMERIC(15,0)   ENCODE az64
	,legal_entity_id NUMERIC(15,0)   ENCODE az64
	,payment_trxn_extension_id NUMERIC(15,0)   ENCODE az64
	,ax_accounted_flag VARCHAR(1)   ENCODE lzo
	,old_customer_bank_account_id NUMERIC(15,0)   ENCODE az64
	,cash_appln_owner_id NUMERIC(15,0)   ENCODE az64
	,work_item_assignment_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,work_item_review_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,work_item_status_code VARCHAR(30)   ENCODE lzo
	,work_item_review_note VARCHAR(2000)   ENCODE lzo
	,prev_pay_from_customer NUMERIC(15,0)   ENCODE az64
	,prev_customer_site_use_id NUMERIC(15,0)   ENCODE az64
	,work_item_exception_reason VARCHAR(30)   ENCODE lzo 
	,AUTOMATCH_SET_ID NUMERIC(15,0)   ENCODE az64
	,AUTOAPPLY_FLAG VARCHAR(1)   ENCODE lzo 
	,SEQ_TYPE_LAST VARCHAR(1)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;

INSERT INTO bec_ods.AR_CASH_RECEIPTS_ALL (
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
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
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
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.AR_CASH_RECEIPTS_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ar_cash_receipts_all';
	
commit;