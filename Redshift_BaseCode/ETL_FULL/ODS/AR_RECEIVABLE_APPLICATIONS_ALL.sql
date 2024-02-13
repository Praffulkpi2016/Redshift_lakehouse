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

DROP TABLE if exists bec_ods.AR_RECEIVABLE_APPLICATIONS_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.ar_receivable_applications_all
(
	receivable_application_id NUMERIC(15,0)   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,amount_applied NUMERIC(28,10)   ENCODE az64
	,gl_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,code_combination_id NUMERIC(15,0)   ENCODE az64
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,display VARCHAR(1)   ENCODE lzo
	,apply_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,application_type VARCHAR(20)   ENCODE lzo
	,status VARCHAR(30)   ENCODE lzo
	,payment_schedule_id NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,cash_receipt_id NUMERIC(15,0)   ENCODE az64
	,applied_customer_trx_id NUMERIC(15,0)   ENCODE az64
	,applied_customer_trx_line_id NUMERIC(15,0)   ENCODE az64
	,applied_payment_schedule_id NUMERIC(15,0)   ENCODE az64
	,customer_trx_id NUMERIC(15,0)   ENCODE az64
	,line_applied NUMERIC(28,10)   ENCODE az64
	,tax_applied NUMERIC(28,10)   ENCODE az64
	,freight_applied NUMERIC(28,10)   ENCODE az64
	,receivables_charges_applied NUMERIC(28,10)   ENCODE az64
	,on_account_customer NUMERIC(15,0)   ENCODE az64
	,receivables_trx_id NUMERIC(15,0)   ENCODE az64
	,earned_discount_taken NUMERIC(28,10)   ENCODE az64
	,unearned_discount_taken NUMERIC(28,10)   ENCODE az64
	,days_late NUMERIC(15,0)   ENCODE az64
	,application_rule VARCHAR(30)   ENCODE lzo
	,gl_posted_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,comments VARCHAR(240)   ENCODE lzo
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
	,postable VARCHAR(1)   ENCODE lzo
	,posting_control_id NUMERIC(15,0)   ENCODE az64
	,acctd_amount_applied_from NUMERIC(28,10)   ENCODE az64
	,acctd_amount_applied_to NUMERIC(28,10)   ENCODE az64
	,acctd_earned_discount_taken NUMERIC(28,10)   ENCODE az64
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,confirmed_flag VARCHAR(1)   ENCODE lzo
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,ussgl_transaction_code_context VARCHAR(30)   ENCODE lzo
	,earned_discount_ccid NUMERIC(15,0)   ENCODE az64
	,unearned_discount_ccid NUMERIC(15,0)   ENCODE az64
	,acctd_unearned_discount_taken NUMERIC(28,10)   ENCODE az64
	,reversal_gl_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cash_receipt_history_id NUMERIC(15,0)   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,tax_code VARCHAR(50)   ENCODE lzo
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
	,cons_inv_id NUMERIC(15,0)   ENCODE az64
	,cons_inv_id_to NUMERIC(15,0)   ENCODE az64
	,amount_applied_from NUMERIC(28,10)   ENCODE az64
	,trans_to_receipt_rate NUMERIC(28,10)   ENCODE az64
	,rule_set_id NUMERIC(15,0)   ENCODE az64
	,line_ediscounted NUMERIC(28,10)   ENCODE az64
	,tax_ediscounted NUMERIC(28,10)   ENCODE az64
	,freight_ediscounted NUMERIC(28,10)   ENCODE az64
	,charges_ediscounted NUMERIC(28,10)   ENCODE az64
	,line_uediscounted NUMERIC(28,10)   ENCODE az64
	,tax_uediscounted NUMERIC(28,10)   ENCODE az64
	,freight_uediscounted NUMERIC(28,10)   ENCODE az64
	,charges_uediscounted NUMERIC(28,10)   ENCODE az64
	,mrc_amount_applied VARCHAR(2000)   ENCODE lzo
	,mrc_amount_applied_from VARCHAR(2000)   ENCODE lzo
	,mrc_display VARCHAR(2000)   ENCODE lzo
	,mrc_status VARCHAR(2000)   ENCODE lzo
	,mrc_payment_schedule_id VARCHAR(2000)   ENCODE lzo
	,mrc_cash_receipt_id VARCHAR(2000)   ENCODE lzo
	,mrc_gl_posted_date VARCHAR(2000)   ENCODE lzo
	,mrc_posting_control_id VARCHAR(2000)   ENCODE lzo
	,mrc_acctd_amount_applied_from VARCHAR(2000)   ENCODE lzo
	,mrc_acctd_amount_applied_to VARCHAR(2000)   ENCODE lzo
	,mrc_acctd_earned_disc_taken VARCHAR(2000)   ENCODE lzo
	,mrc_acctd_unearned_disc_taken VARCHAR(2000)   ENCODE lzo
	,edisc_tax_acct_rule VARCHAR(3)   ENCODE lzo
	,unedisc_tax_acct_rule VARCHAR(3)   ENCODE lzo
	,link_to_trx_hist_id NUMERIC(15,0)   ENCODE az64
	,link_to_customer_trx_id NUMERIC(15,0)   ENCODE az64
	,application_ref_type VARCHAR(30)   ENCODE lzo
	,application_ref_id NUMERIC(15,0)   ENCODE az64
	,application_ref_num VARCHAR(150)   ENCODE lzo
	,chargeback_customer_trx_id NUMERIC(15,0)   ENCODE az64
	,secondary_application_ref_id NUMERIC(15,0)   ENCODE az64
	,payment_set_id NUMERIC(15,0)   ENCODE az64
	,application_ref_reason VARCHAR(30)   ENCODE lzo
	,customer_reference VARCHAR(100)   ENCODE lzo
	,customer_reason VARCHAR(30)   ENCODE lzo
	,applied_rec_app_id NUMERIC(15,0)   ENCODE az64
	,secondary_application_ref_type VARCHAR(30)   ENCODE lzo
	,secondary_application_ref_num VARCHAR(150)   ENCODE lzo
	,event_id NUMERIC(15,0)   ENCODE az64
	,upgrade_method VARCHAR(30)   ENCODE lzo
	,ax_accounted_flag VARCHAR(1)   ENCODE lzo 
	,INCLUDE_IN_ACCUMULATION VARCHAR(1)   ENCODE lzo
	,ON_ACCT_CUST_ID NUMERIC(15,0)   ENCODE az64
	,ON_ACCT_CUST_SITE_USE_ID NUMERIC(15,0)   ENCODE az64
	,ON_ACCT_PO_NUM	VARCHAR(50)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;

INSERT INTO bec_ods.AR_RECEIVABLE_APPLICATIONS_ALL (
    receivable_application_id,
	last_updated_by,
	last_update_date,
	created_by,
	creation_date,
	amount_applied,
	gl_date,
	code_combination_id,
	set_of_books_id,
	display,
	apply_date,
	application_type,
	status,
	payment_schedule_id,
	last_update_login,
	cash_receipt_id,
	applied_customer_trx_id,
	applied_customer_trx_line_id,
	applied_payment_schedule_id,
	customer_trx_id,
	line_applied,
	tax_applied,
	freight_applied,
	receivables_charges_applied,
	on_account_customer,
	receivables_trx_id,
	earned_discount_taken,
	unearned_discount_taken,
	days_late,
	application_rule,
	gl_posted_date,
	comments,
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
	postable,
	posting_control_id,
	acctd_amount_applied_from,
	acctd_amount_applied_to,
	acctd_earned_discount_taken,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	confirmed_flag,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	ussgl_transaction_code,
	ussgl_transaction_code_context,
	earned_discount_ccid,
	unearned_discount_ccid,
	acctd_unearned_discount_taken,
	reversal_gl_date, 
	cash_receipt_history_id,
	org_id,
	tax_code,
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
	cons_inv_id,
	cons_inv_id_to,
	amount_applied_from,
	trans_to_receipt_rate,
	rule_set_id,
	line_ediscounted,
	tax_ediscounted,
	freight_ediscounted,
	charges_ediscounted,
	line_uediscounted,
	tax_uediscounted,
	freight_uediscounted,
	charges_uediscounted,
	mrc_amount_applied,
	mrc_amount_applied_from,
	mrc_display,
	mrc_status,
	mrc_payment_schedule_id,
	mrc_cash_receipt_id,
	mrc_gl_posted_date,
	mrc_posting_control_id,
	mrc_acctd_amount_applied_from,
	mrc_acctd_amount_applied_to,
	mrc_acctd_earned_disc_taken,
	mrc_acctd_unearned_disc_taken,
	edisc_tax_acct_rule,
	unedisc_tax_acct_rule,
	link_to_trx_hist_id,
	link_to_customer_trx_id,
	application_ref_type,
	application_ref_id,
	application_ref_num,
	chargeback_customer_trx_id,
	secondary_application_ref_id,
	payment_set_id,
	application_ref_reason,
	customer_reference,
	customer_reason,
	applied_rec_app_id,
	secondary_application_ref_type,
	secondary_application_ref_num,
	event_id,
	upgrade_method,
	ax_accounted_flag, 
	include_in_accumulation,
	on_acct_cust_id,
	on_acct_cust_site_use_id,
	on_acct_po_num,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        receivable_application_id,
	last_updated_by,
	last_update_date,
	created_by,
	creation_date,
	amount_applied,
	gl_date,
	code_combination_id,
	set_of_books_id,
	display,
	apply_date,
	application_type,
	status,
	payment_schedule_id,
	last_update_login,
	cash_receipt_id,
	applied_customer_trx_id,
	applied_customer_trx_line_id,
	applied_payment_schedule_id,
	customer_trx_id,
	line_applied,
	tax_applied,
	freight_applied,
	receivables_charges_applied,
	on_account_customer,
	receivables_trx_id,
	earned_discount_taken,
	unearned_discount_taken,
	days_late,
	application_rule,
	gl_posted_date,
	comments,
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
	postable,
	posting_control_id,
	acctd_amount_applied_from,
	acctd_amount_applied_to,
	acctd_earned_discount_taken,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	confirmed_flag,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	ussgl_transaction_code,
	ussgl_transaction_code_context,
	earned_discount_ccid,
	unearned_discount_ccid,
	acctd_unearned_discount_taken,
	reversal_gl_date, 
	cash_receipt_history_id,
	org_id,
	tax_code,
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
	cons_inv_id,
	cons_inv_id_to,
	amount_applied_from,
	trans_to_receipt_rate,
	rule_set_id,
	line_ediscounted,
	tax_ediscounted,
	freight_ediscounted,
	charges_ediscounted,
	line_uediscounted,
	tax_uediscounted,
	freight_uediscounted,
	charges_uediscounted,
	mrc_amount_applied,
	mrc_amount_applied_from,
	mrc_display,
	mrc_status,
	mrc_payment_schedule_id,
	mrc_cash_receipt_id,
	mrc_gl_posted_date,
	mrc_posting_control_id,
	mrc_acctd_amount_applied_from,
	mrc_acctd_amount_applied_to,
	mrc_acctd_earned_disc_taken,
	mrc_acctd_unearned_disc_taken,
	edisc_tax_acct_rule,
	unedisc_tax_acct_rule,
	link_to_trx_hist_id,
	link_to_customer_trx_id,
	application_ref_type,
	application_ref_id,
	application_ref_num,
	chargeback_customer_trx_id,
	secondary_application_ref_id,
	payment_set_id,
	application_ref_reason,
	customer_reference,
	customer_reason,
	applied_rec_app_id,
	secondary_application_ref_type,
	secondary_application_ref_num,
	event_id,
	upgrade_method,
	ax_accounted_flag, 
	include_in_accumulation,
	on_acct_cust_id,
	on_acct_cust_site_use_id,
	on_acct_po_num,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.AR_RECEIVABLE_APPLICATIONS_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ar_receivable_applications_all';
	
commit;