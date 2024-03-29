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

delete from bec_ods.AR_PAYMENT_SCHEDULES_ALL
where payment_schedule_id in (
select stg.payment_schedule_id from bec_ods.AR_PAYMENT_SCHEDULES_ALL ods, bec_ods_stg.AR_PAYMENT_SCHEDULES_ALL stg
where ods.payment_schedule_id = stg.payment_schedule_id and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.AR_PAYMENT_SCHEDULES_ALL
       (
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
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
(
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
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.AR_PAYMENT_SCHEDULES_ALL
	where kca_operation IN ('INSERT','UPDATE') 
	and (payment_schedule_id,kca_seq_id) in 
	(select payment_schedule_id,max(kca_seq_id) from bec_ods_stg.AR_PAYMENT_SCHEDULES_ALL 
     where kca_operation IN ('INSERT','UPDATE')
     group by payment_schedule_id)
);

commit;

-- Soft delete
update bec_ods.AR_PAYMENT_SCHEDULES_ALL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.AR_PAYMENT_SCHEDULES_ALL set IS_DELETED_FLG = 'Y'
where (payment_schedule_id)  in
(
select payment_schedule_id from bec_raw_dl_ext.AR_PAYMENT_SCHEDULES_ALL
where (payment_schedule_id,KCA_SEQ_ID)
in 
(
select payment_schedule_id,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.AR_PAYMENT_SCHEDULES_ALL
group by payment_schedule_id
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'ar_payment_schedules_all';

commit;