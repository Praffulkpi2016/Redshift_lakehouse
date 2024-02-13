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

DROP TABLE if exists bec_ods.AR_PAYMENT_SCHEDULES_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.ar_payment_schedules_all
(
	payment_schedule_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,due_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,amount_due_original NUMERIC(28,10)   ENCODE az64
	,amount_due_remaining NUMERIC(28,10)   ENCODE az64
	,number_of_due_dates NUMERIC(15,0)   ENCODE az64
	,status VARCHAR(30)   ENCODE lzo
	,invoice_currency_code VARCHAR(15)   ENCODE lzo
	,"class" VARCHAR(20)   ENCODE lzo
	,cust_trx_type_id NUMERIC(15,0)   ENCODE az64
	,customer_id NUMERIC(15,0)   ENCODE az64
	,customer_site_use_id NUMERIC(15,0)   ENCODE az64
	,customer_trx_id NUMERIC(15,0)   ENCODE az64
	,cash_receipt_id NUMERIC(15,0)   ENCODE az64
	,associated_cash_receipt_id NUMERIC(15,0)   ENCODE az64
	,term_id NUMERIC(15,0)   ENCODE az64
	,terms_sequence_number NUMERIC(15,0)   ENCODE az64
	,gl_date_closed TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,actual_date_closed TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,discount_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,amount_line_items_original NUMERIC(28,10)   ENCODE az64
	,amount_line_items_remaining NUMERIC(28,10)   ENCODE az64
	,amount_applied NUMERIC(28,10)   ENCODE az64
	,amount_adjusted NUMERIC(28,10)   ENCODE az64
	,amount_in_dispute NUMERIC(28,10)   ENCODE az64
	,amount_credited NUMERIC(28,10)   ENCODE az64
	,receivables_charges_charged NUMERIC(28,10)   ENCODE az64
	,receivables_charges_remaining NUMERIC(28,10)   ENCODE az64
	,freight_original NUMERIC(28,10)   ENCODE az64
	,freight_remaining NUMERIC(28,10)   ENCODE az64
	,tax_original NUMERIC(28,10)   ENCODE az64
	,tax_remaining NUMERIC(28,10)   ENCODE az64
	,discount_original NUMERIC(28,10)   ENCODE az64
	,discount_remaining NUMERIC(28,10)   ENCODE az64
	,discount_taken_earned NUMERIC(28,10)   ENCODE az64
	,discount_taken_unearned NUMERIC(28,10)   ENCODE az64
	,in_collection VARCHAR(1)   ENCODE lzo
	,cash_applied_id_last NUMERIC(15,0)   ENCODE az64
	,cash_applied_date_last TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cash_applied_amount_last NUMERIC(28,10)   ENCODE az64
	,cash_applied_status_last VARCHAR(30)   ENCODE lzo
	,cash_gl_date_last TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cash_receipt_id_last NUMERIC(15,0)   ENCODE az64
	,cash_receipt_date_last TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cash_receipt_amount_last NUMERIC(28,10)   ENCODE az64
	,cash_receipt_status_last VARCHAR(30)   ENCODE lzo
	,exchange_rate_type VARCHAR(30)   ENCODE lzo
	,exchange_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,exchange_rate NUMERIC(28,10)   ENCODE az64
	,adjustment_id_last NUMERIC(15,0)   ENCODE az64
	,adjustment_date_last TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,adjustment_gl_date_last TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,adjustment_amount_last NUMERIC(28,10)   ENCODE az64
	,follow_up_date_last TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,follow_up_code_last VARCHAR(30)   ENCODE lzo
	,promise_date_last TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,promise_amount_last NUMERIC(28,10)   ENCODE az64
	,collector_last NUMERIC(15,0)   ENCODE az64
	,call_date_last TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,trx_number VARCHAR(30)   ENCODE lzo
	,trx_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,reversed_cash_receipt_id NUMERIC(15,0)   ENCODE az64
	,amount_adjusted_pending NUMERIC(28,10)   ENCODE az64
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,gl_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,acctd_amount_due_remaining NUMERIC(28,10)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,receipt_confirmed_flag VARCHAR(1)   ENCODE lzo
	,request_id NUMERIC(15,0)   ENCODE az64
	,selected_for_receipt_batch_id NUMERIC(15,0)   ENCODE az64
	,last_charge_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,second_last_charge_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,dispute_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,staged_dunning_level NUMERIC(2,0)   ENCODE az64
	,dunning_level_override_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,cons_inv_id_rev NUMERIC(15,0)   ENCODE az64
	,exclude_from_dunning_flag VARCHAR(1)   ENCODE lzo
	,mrc_customer_trx_id VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_rate_type VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_date VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_rate VARCHAR(2000)   ENCODE lzo
	,mrc_acctd_amount_due_remaining VARCHAR(2000)   ENCODE lzo
	,br_amount_assigned NUMERIC(28,10)   ENCODE az64
	,reserved_type VARCHAR(30)   ENCODE lzo
	,reserved_value NUMERIC(15,0)   ENCODE az64
	,active_claim_flag VARCHAR(1)   ENCODE lzo
	,exclude_from_cons_bill_flag VARCHAR(1)   ENCODE lzo
	,payment_approval VARCHAR(30)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;

INSERT INTO bec_ods.AR_PAYMENT_SCHEDULES_ALL (
    payment_schedule_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	due_date,
	amount_due_original,
	amount_due_remaining,
	number_of_due_dates,
	status,
	invoice_currency_code,
	"class",
	cust_trx_type_id,
	customer_id,
	customer_site_use_id,
	customer_trx_id,
	cash_receipt_id,
	associated_cash_receipt_id,
	term_id,
	terms_sequence_number,
	gl_date_closed,
	actual_date_closed,
	discount_date,
	amount_line_items_original,
	amount_line_items_remaining,
	amount_applied,
	amount_adjusted,
	amount_in_dispute,
	amount_credited,
	receivables_charges_charged,
	receivables_charges_remaining,
	freight_original,
	freight_remaining,
	tax_original,
	tax_remaining,
	discount_original,
	discount_remaining,
	discount_taken_earned,
	discount_taken_unearned,
	in_collection,
	cash_applied_id_last,
	cash_applied_date_last,
	cash_applied_amount_last,
	cash_applied_status_last,
	cash_gl_date_last,
	cash_receipt_id_last,
	cash_receipt_date_last,
	cash_receipt_amount_last,
	cash_receipt_status_last,
	exchange_rate_type,
	exchange_date,
	exchange_rate,
	adjustment_id_last,
	adjustment_date_last,
	adjustment_gl_date_last,
	adjustment_amount_last,
	follow_up_date_last,
	follow_up_code_last,
	promise_date_last,
	promise_amount_last,
	collector_last,
	call_date_last,
	trx_number,
	trx_date,
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
	reversed_cash_receipt_id,
	amount_adjusted_pending,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	gl_date,
	acctd_amount_due_remaining,
	program_application_id,
	program_id,
	program_update_date,
	receipt_confirmed_flag,
	request_id,
	selected_for_receipt_batch_id,
	last_charge_date,
	second_last_charge_date,
	dispute_date,
	org_id,
	staged_dunning_level,
	dunning_level_override_date,
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
	cons_inv_id_rev,
	exclude_from_dunning_flag,
	mrc_customer_trx_id,
	mrc_exchange_rate_type,
	mrc_exchange_date,
	mrc_exchange_rate,
	mrc_acctd_amount_due_remaining,
	br_amount_assigned,
	reserved_type,
	reserved_value,
	active_claim_flag,
	exclude_from_cons_bill_flag,
	payment_approval,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        payment_schedule_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	due_date,
	amount_due_original,
	amount_due_remaining,
	number_of_due_dates,
	status,
	invoice_currency_code,
	"class",
	cust_trx_type_id,
	customer_id,
	customer_site_use_id,
	customer_trx_id,
	cash_receipt_id,
	associated_cash_receipt_id,
	term_id,
	terms_sequence_number,
	gl_date_closed,
	actual_date_closed,
	discount_date,
	amount_line_items_original,
	amount_line_items_remaining,
	amount_applied,
	amount_adjusted,
	amount_in_dispute,
	amount_credited,
	receivables_charges_charged,
	receivables_charges_remaining,
	freight_original,
	freight_remaining,
	tax_original,
	tax_remaining,
	discount_original,
	discount_remaining,
	discount_taken_earned,
	discount_taken_unearned,
	in_collection,
	cash_applied_id_last,
	cash_applied_date_last,
	cash_applied_amount_last,
	cash_applied_status_last,
	cash_gl_date_last,
	cash_receipt_id_last,
	cash_receipt_date_last,
	cash_receipt_amount_last,
	cash_receipt_status_last,
	exchange_rate_type,
	exchange_date,
	exchange_rate,
	adjustment_id_last,
	adjustment_date_last,
	adjustment_gl_date_last,
	adjustment_amount_last,
	follow_up_date_last,
	follow_up_code_last,
	promise_date_last,
	promise_amount_last,
	collector_last,
	call_date_last,
	trx_number,
	trx_date,
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
	reversed_cash_receipt_id,
	amount_adjusted_pending,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	gl_date,
	acctd_amount_due_remaining,
	program_application_id,
	program_id,
	program_update_date,
	receipt_confirmed_flag,
	request_id,
	selected_for_receipt_batch_id,
	last_charge_date,
	second_last_charge_date,
	dispute_date,
	org_id,
	staged_dunning_level,
	dunning_level_override_date,
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
	cons_inv_id_rev,
	exclude_from_dunning_flag,
	mrc_customer_trx_id,
	mrc_exchange_rate_type,
	mrc_exchange_date,
	mrc_exchange_rate,
	mrc_acctd_amount_due_remaining,
	br_amount_assigned,
	reserved_type,
	reserved_value,
	active_claim_flag,
	exclude_from_cons_bill_flag,
	payment_approval,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.AR_PAYMENT_SCHEDULES_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ar_payment_schedules_all';
	
commit;