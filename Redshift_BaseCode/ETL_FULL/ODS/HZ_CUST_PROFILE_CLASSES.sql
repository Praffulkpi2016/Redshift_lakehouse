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

DROP TABLE if exists bec_ods.hz_cust_profile_classes;

CREATE TABLE IF NOT EXISTS bec_ods.hz_cust_profile_classes
(
	profile_class_id NUMERIC(15,0)   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,name VARCHAR(30)   ENCODE lzo
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,status VARCHAR(1)   ENCODE lzo
	,wh_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,collector_id NUMERIC(15,0)   ENCODE az64
	,credit_analyst_id NUMERIC(15,0)   ENCODE az64
	,credit_checking VARCHAR(1)   ENCODE lzo
	,tolerance NUMERIC(15,0)   ENCODE az64
	,discount_terms VARCHAR(1)   ENCODE lzo
	,dunning_letters VARCHAR(1)   ENCODE lzo
	,interest_charges VARCHAR(1)   ENCODE lzo
	,pref_functional_currency VARCHAR(30)   ENCODE lzo
	,statements VARCHAR(1)   ENCODE lzo
	,credit_balance_statements VARCHAR(1)   ENCODE lzo
	,description VARCHAR(80)   ENCODE lzo
	,review_cycle_days NUMERIC(15,0)   ENCODE az64
	,outside_reporting VARCHAR(1)   ENCODE lzo
	,standard_terms NUMERIC(15,0)   ENCODE az64
	,override_terms VARCHAR(1)   ENCODE lzo
	,dunning_letter_set_id NUMERIC(15,0)   ENCODE az64
	,combine_dunning_letters VARCHAR(1)   ENCODE lzo
	,interest_period_days NUMERIC(15,0)   ENCODE az64
	,autocash_hierarchy_id NUMERIC(15,0)   ENCODE az64
	,payment_grace_days NUMERIC(15,0)   ENCODE az64
	,discount_grace_days NUMERIC(15,0)   ENCODE az64
	,statement_cycle_id NUMERIC(15,0)   ENCODE az64
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
	,request_id NUMERIC(15,0)   ENCODE az64
	,copy_method VARCHAR(9)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,auto_rec_incl_disputed_flag VARCHAR(1)   ENCODE lzo
	,tax_printing_option VARCHAR(30)   ENCODE lzo
	,charge_on_finance_charge_flag VARCHAR(1)   ENCODE lzo
	,grouping_rule_id NUMERIC(15,0)   ENCODE az64
	,jgzz_attribute_category VARCHAR(30)   ENCODE lzo
	,jgzz_attribute1 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute2 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute3 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute4 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute5 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute6 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute7 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute8 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute9 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute10 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute11 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute12 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute13 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute14 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute15 VARCHAR(150)   ENCODE lzo
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
	,cons_inv_flag VARCHAR(1)   ENCODE lzo
	,cons_inv_type VARCHAR(30)   ENCODE lzo
	,autocash_hierarchy_id_for_adr NUMERIC(15,0)   ENCODE az64
	,lockbox_matching_option VARCHAR(20)   ENCODE lzo
	,review_cycle VARCHAR(30)   ENCODE lzo
	,credit_classification VARCHAR(30)   ENCODE lzo
	,cons_bill_level VARCHAR(30)   ENCODE lzo
	,late_charge_calculation_trx VARCHAR(30)   ENCODE lzo
	,credit_items_flag VARCHAR(1)   ENCODE lzo
	,disputed_transactions_flag VARCHAR(1)   ENCODE lzo
	,late_charge_type VARCHAR(30)   ENCODE lzo
	,late_charge_term_id NUMERIC(15,0)   ENCODE az64
	,interest_calculation_period VARCHAR(30)   ENCODE lzo
	,hold_charged_invoices_flag VARCHAR(1)   ENCODE lzo
	,message_text_id NUMERIC(15,0)   ENCODE az64
	,multiple_interest_rates_flag VARCHAR(1)   ENCODE lzo
	,charge_begin_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,object_version_number NUMERIC(15,0)   ENCODE az64
	,AUTOMATCH_SET_ID NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert
	into
	bec_ods.hz_cust_profile_classes
(	profile_class_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	program_application_id,
	"name",
	program_id,
	program_update_date,
	status,
	wh_update_date,
	collector_id,
	credit_analyst_id,
	credit_checking,
	tolerance,
	discount_terms,
	dunning_letters,
	interest_charges,
	pref_functional_currency,
	statements,
	credit_balance_statements,
	description,
	review_cycle_days,
	outside_reporting,
	standard_terms,
	override_terms,
	dunning_letter_set_id,
	combine_dunning_letters,
	interest_period_days,
	autocash_hierarchy_id,
	payment_grace_days,
	discount_grace_days,
	statement_cycle_id,
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
	request_id,
	copy_method,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	auto_rec_incl_disputed_flag,
	tax_printing_option,
	charge_on_finance_charge_flag,
	grouping_rule_id,
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
	review_cycle,
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
	object_version_number,
	AUTOMATCH_SET_ID,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
	SELECT
	profile_class_id,
	last_updated_by,
	last_update_date,
	last_update_login,
	created_by,
	creation_date,
	program_application_id,
	"name",
	program_id,
	program_update_date,
	status,
	wh_update_date,
	collector_id,
	credit_analyst_id,
	credit_checking,
	tolerance,
	discount_terms,
	dunning_letters,
	interest_charges,
	pref_functional_currency,
	statements,
	credit_balance_statements,
	description,
	review_cycle_days,
	outside_reporting,
	standard_terms,
	override_terms,
	dunning_letter_set_id,
	combine_dunning_letters,
	interest_period_days,
	autocash_hierarchy_id,
	payment_grace_days,
	discount_grace_days,
	statement_cycle_id,
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
	request_id,
	copy_method,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	auto_rec_incl_disputed_flag,
	tax_printing_option,
	charge_on_finance_charge_flag,
	grouping_rule_id,
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
	review_cycle,
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
	object_version_number,
	AUTOMATCH_SET_ID,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.HZ_CUST_PROFILE_CLASSES;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'hz_cust_profile_classes';
	
commit;
	
