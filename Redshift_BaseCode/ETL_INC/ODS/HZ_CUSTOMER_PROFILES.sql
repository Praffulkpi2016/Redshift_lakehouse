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

delete from bec_ods.hz_customer_profiles
where (CUST_ACCOUNT_PROFILE_ID) in (
select stg.CUST_ACCOUNT_PROFILE_ID from bec_ods.hz_customer_profiles ods, bec_ods_stg.hz_customer_profiles stg
where ods.CUST_ACCOUNT_PROFILE_ID = stg.CUST_ACCOUNT_PROFILE_ID  and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert
	into
	bec_ods.hz_customer_profiles
(cust_account_profile_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	cust_account_id,
	status,
	collector_id,
	credit_analyst_id,
	credit_checking,
	next_credit_review_date,
	tolerance,
	discount_terms,
	dunning_letters,
	interest_charges,
	pref_functional_currency,
	send_statements,
	credit_balance_statements,
	credit_hold,
	profile_class_id,
	site_use_id,
	credit_rating,
	risk_code,
	standard_terms,
	override_terms,
	dunning_letter_set_id,
	interest_period_days,
	payment_grace_days,
	discount_grace_days,
	statement_cycle_id,
	account_status,
	percent_collectable,
	autocash_hierarchy_id,
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
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	wh_update_date,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	auto_rec_incl_disputed_flag,
	tax_printing_option,
	charge_on_finance_charge_flag,
	grouping_rule_id,
	clearing_days,
	jgzz_attribute_category,
	jgzz_attribute1,
	jgzz_attribute2,
	jgzz_attribute3,
	jgzz_attribute4,
	jgzz_attribute5,
	jgzz_attribute6,
	jgzz_attribute7,
	jgzz_attribute8,
	jgzz_attribute9,
	jgzz_attribute10,
	jgzz_attribute11,
	jgzz_attribute12,
	jgzz_attribute13,
	jgzz_attribute14,
	jgzz_attribute15,
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
	cons_inv_flag,
	cons_inv_type,
	autocash_hierarchy_id_for_adr,
	lockbox_matching_option,
	object_version_number,
	created_by_module,
	application_id,
	review_cycle,
	last_credit_review_date,
	party_id,
	credit_classification,
	cons_bill_level,
	late_charge_calculation_trx,
	credit_items_flag,
	disputed_transactions_flag,
	late_charge_type,
	late_charge_term_id,
	interest_calculation_period,
	hold_charged_invoices_flag,
	message_text_id,
	multiple_interest_rates_flag,
	charge_begin_date,
	AUTOMATCH_SET_ID,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
	SELECT
	cust_account_profile_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	cust_account_id,
	status,
	collector_id,
	credit_analyst_id,
	credit_checking,
	next_credit_review_date,
	tolerance,
	discount_terms,
	dunning_letters,
	interest_charges,
	pref_functional_currency,
	send_statements,
	credit_balance_statements,
	credit_hold,
	profile_class_id,
	site_use_id,
	credit_rating,
	risk_code,
	standard_terms,
	override_terms,
	dunning_letter_set_id,
	interest_period_days,
	payment_grace_days,
	discount_grace_days,
	statement_cycle_id,
	account_status,
	percent_collectable,
	autocash_hierarchy_id,
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
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	wh_update_date,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	auto_rec_incl_disputed_flag,
	tax_printing_option,
	charge_on_finance_charge_flag,
	grouping_rule_id,
	clearing_days,
	jgzz_attribute_category,
	jgzz_attribute1,
	jgzz_attribute2,
	jgzz_attribute3,
	jgzz_attribute4,
	jgzz_attribute5,
	jgzz_attribute6,
	jgzz_attribute7,
	jgzz_attribute8,
	jgzz_attribute9,
	jgzz_attribute10,
	jgzz_attribute11,
	jgzz_attribute12,
	jgzz_attribute13,
	jgzz_attribute14,
	jgzz_attribute15,
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
	cons_inv_flag,
	cons_inv_type,
	autocash_hierarchy_id_for_adr,
	lockbox_matching_option,
	object_version_number,
	created_by_module,
	application_id,
	review_cycle,
	last_credit_review_date,
	party_id,
	credit_classification,
	cons_bill_level,
	late_charge_calculation_trx,
	credit_items_flag,
	disputed_transactions_flag,
	late_charge_type,
	late_charge_term_id,
	interest_calculation_period,
	hold_charged_invoices_flag,
	message_text_id,
	multiple_interest_rates_flag,
	charge_begin_date,
	AUTOMATCH_SET_ID,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.hz_customer_profiles
	where kca_operation IN ('INSERT','UPDATE') 
	and (CUST_ACCOUNT_PROFILE_ID,kca_seq_id) in 
	(select CUST_ACCOUNT_PROFILE_ID,max(kca_seq_id) from bec_ods_stg.hz_customer_profiles 
     where kca_operation IN ('INSERT','UPDATE')
     group by CUST_ACCOUNT_PROFILE_ID);

commit;
 

-- Soft delete
update bec_ods.hz_customer_profiles set IS_DELETED_FLG = 'N';
commit;
update bec_ods.hz_customer_profiles set IS_DELETED_FLG = 'Y'
where (CUST_ACCOUNT_PROFILE_ID)  in
(
select CUST_ACCOUNT_PROFILE_ID from bec_raw_dl_ext.hz_customer_profiles
where (CUST_ACCOUNT_PROFILE_ID,KCA_SEQ_ID)
in 
(
select CUST_ACCOUNT_PROFILE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.hz_customer_profiles
group by CUST_ACCOUNT_PROFILE_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'hz_customer_profiles';

commit;