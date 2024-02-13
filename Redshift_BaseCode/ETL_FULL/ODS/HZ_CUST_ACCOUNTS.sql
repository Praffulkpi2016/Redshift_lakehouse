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

DROP TABLE if exists bec_ods.HZ_CUST_ACCOUNTS;

CREATE TABLE IF NOT EXISTS bec_ods.hz_cust_accounts
(
	cust_account_id NUMERIC(15,0)   ENCODE az64
	,party_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,account_number VARCHAR(30)   ENCODE lzo
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,wh_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,attribute16 VARCHAR(150)   ENCODE lzo
	,attribute17 VARCHAR(150)   ENCODE lzo
	,attribute18 VARCHAR(150)   ENCODE lzo
	,attribute19 VARCHAR(150)   ENCODE lzo
	,attribute20 VARCHAR(150)   ENCODE lzo
	,global_attribute_category VARCHAR(30)   ENCODE lzo
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
	,orig_system_reference VARCHAR(240)   ENCODE lzo
	,status VARCHAR(1)   ENCODE lzo
	,customer_type VARCHAR(30)   ENCODE lzo
	,customer_class_code VARCHAR(30)   ENCODE lzo
	,primary_salesrep_id NUMERIC(15,0)   ENCODE az64
	,sales_channel_code VARCHAR(30)   ENCODE lzo
	,order_type_id NUMERIC(15,0)   ENCODE az64
	,price_list_id NUMERIC(15,0)   ENCODE az64
	,subcategory_code VARCHAR(30)   ENCODE lzo
	,tax_code VARCHAR(50)   ENCODE lzo
	,fob_point VARCHAR(30)   ENCODE lzo
	,freight_term VARCHAR(30)   ENCODE lzo
	,ship_partial VARCHAR(1)   ENCODE lzo
	,ship_via VARCHAR(30)   ENCODE lzo
	,warehouse_id NUMERIC(15,0)   ENCODE az64
	,payment_term_id NUMERIC(15,0)   ENCODE az64
	,tax_header_level_flag VARCHAR(1)   ENCODE lzo
	,tax_rounding_rule VARCHAR(30)   ENCODE lzo
	,coterminate_day_month VARCHAR(6)   ENCODE lzo
	,primary_specialist_id NUMERIC(15,0)   ENCODE az64
	,secondary_specialist_id NUMERIC(15,0)   ENCODE az64
	,account_liable_flag VARCHAR(1)   ENCODE lzo
	,restriction_limit_amount NUMERIC(28,10)   ENCODE az64
	,current_balance NUMERIC(28,10)   ENCODE az64
	,password_text VARCHAR(60)   ENCODE lzo
	,high_priority_indicator VARCHAR(1)   ENCODE lzo
	,account_established_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,account_termination_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,account_activation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,credit_classification_code VARCHAR(30)   ENCODE lzo
	,department VARCHAR(30)   ENCODE lzo
	,major_account_number VARCHAR(30)   ENCODE lzo
	,hotwatch_service_flag VARCHAR(1)   ENCODE lzo
	,hotwatch_svc_bal_ind VARCHAR(30)   ENCODE lzo
	,held_bill_expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,hold_bill_flag VARCHAR(1)   ENCODE lzo
	,high_priority_remarks VARCHAR(80)   ENCODE lzo
	,po_effective_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,po_expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,realtime_rate_flag VARCHAR(1)   ENCODE lzo
	,single_user_flag VARCHAR(1)   ENCODE lzo
	,watch_account_flag VARCHAR(1)   ENCODE lzo
	,watch_balance_indicator VARCHAR(1)   ENCODE lzo
	,geo_code VARCHAR(30)   ENCODE lzo
	,acct_life_cycle_status VARCHAR(30)   ENCODE lzo
	,account_name VARCHAR(240)   ENCODE lzo
	,deposit_refund_method VARCHAR(20)   ENCODE lzo
	,dormant_account_flag VARCHAR(1)   ENCODE lzo
	,npa_number VARCHAR(60)   ENCODE lzo
	,pin_number NUMERIC(16,0)   ENCODE az64
	,suspension_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,write_off_adjustment_amount NUMERIC(28,10)   ENCODE az64
	,write_off_payment_amount NUMERIC(28,10)   ENCODE az64
	,write_off_amount NUMERIC(28,10)   ENCODE az64
	,source_code VARCHAR(150)   ENCODE lzo
	,competitor_type VARCHAR(150)   ENCODE lzo
	,comments VARCHAR(240)   ENCODE lzo
	,dates_negative_tolerance NUMERIC(28,10)   ENCODE az64
	,dates_positive_tolerance NUMERIC(28,10)   ENCODE az64
	,date_type_preference VARCHAR(20)   ENCODE lzo
	,over_shipment_tolerance NUMERIC(28,10)   ENCODE az64
	,under_shipment_tolerance NUMERIC(28,10)   ENCODE az64
	,over_return_tolerance NUMERIC(28,10)   ENCODE az64
	,under_return_tolerance NUMERIC(28,10)   ENCODE az64
	,item_cross_ref_pref VARCHAR(30)   ENCODE lzo
	,ship_sets_include_lines_flag VARCHAR(1)   ENCODE lzo
	,arrivalsets_include_lines_flag VARCHAR(1)   ENCODE lzo
	,sched_date_push_flag VARCHAR(1)   ENCODE lzo
	,invoice_quantity_rule VARCHAR(30)   ENCODE lzo
	,pricing_event VARCHAR(30)   ENCODE lzo
	,account_replication_key NUMERIC(15,0)   ENCODE az64
	,status_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,autopay_flag VARCHAR(1)   ENCODE lzo
	,notify_flag VARCHAR(1)   ENCODE lzo
	,last_batch_id NUMERIC(15,0)   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,object_version_number NUMERIC(15,0)   ENCODE az64
	,created_by_module VARCHAR(150)   ENCODE lzo
	,application_id NUMERIC(15,0)   ENCODE az64
	,selling_party_id NUMERIC(15,0)   ENCODE az64 
	,ADVANCE_PAYMENT_INDICATOR VARCHAR(30) ENCODE lzo
    ,DUNS_EXTENSION VARCHAR(4) ENCODE lzo
    ,FEDERAL_ENTITY_TYPE VARCHAR(30) ENCODE lzo
    ,TRADING_PARTNER_AGENCY_ID NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

INSERT INTO bec_ods.HZ_CUST_ACCOUNTS (
    cust_account_id,
	party_id,
	last_update_date,
	account_number,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	wh_update_date,
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
	attribute16,
	attribute17,
	attribute18,
	attribute19,
	attribute20,
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
	orig_system_reference,
	status,
	customer_type,
	customer_class_code,
	primary_salesrep_id,
	sales_channel_code,
	order_type_id,
	price_list_id,
	subcategory_code,
	tax_code,
	fob_point,
	freight_term,
	ship_partial,
	ship_via,
	warehouse_id,
	payment_term_id,
	tax_header_level_flag,
	tax_rounding_rule,
	coterminate_day_month,
	primary_specialist_id,
	secondary_specialist_id,
	account_liable_flag,
	restriction_limit_amount,
	current_balance,
	password_text,
	high_priority_indicator,
	account_established_date,
	account_termination_date,
	account_activation_date,
	credit_classification_code,
	department,
	major_account_number,
	hotwatch_service_flag,
	hotwatch_svc_bal_ind,
	held_bill_expiration_date,
	hold_bill_flag,
	high_priority_remarks,
	po_effective_date,
	po_expiration_date,
	realtime_rate_flag,
	single_user_flag,
	watch_account_flag,
	watch_balance_indicator,
	geo_code,
	acct_life_cycle_status,
	account_name,
	deposit_refund_method,
	dormant_account_flag,
	npa_number,
	pin_number,
	suspension_date,
	write_off_adjustment_amount,
	write_off_payment_amount,
	write_off_amount,
	source_code,
	competitor_type,
	comments,
	dates_negative_tolerance,
	dates_positive_tolerance,
	date_type_preference,
	over_shipment_tolerance,
	under_shipment_tolerance,
	over_return_tolerance,
	under_return_tolerance,
	item_cross_ref_pref,
	ship_sets_include_lines_flag,
	arrivalsets_include_lines_flag,
	sched_date_push_flag,
	invoice_quantity_rule,
	pricing_event,
	account_replication_key,
	status_update_date,
	autopay_flag,
	notify_flag,
	last_batch_id,
	org_id,
	object_version_number,
	created_by_module,
	application_id,
	selling_party_id, 
	ADVANCE_PAYMENT_INDICATOR,
    DUNS_EXTENSION,
    FEDERAL_ENTITY_TYPE,
    TRADING_PARTNER_AGENCY_ID,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
    SELECT
        cust_account_id,
	party_id,
	last_update_date,
	account_number,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	wh_update_date,
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
	attribute16,
	attribute17,
	attribute18,
	attribute19,
	attribute20,
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
	orig_system_reference,
	status,
	customer_type,
	customer_class_code,
	primary_salesrep_id,
	sales_channel_code,
	order_type_id,
	price_list_id,
	subcategory_code,
	tax_code,
	fob_point,
	freight_term,
	ship_partial,
	ship_via,
	warehouse_id,
	payment_term_id,
	tax_header_level_flag,
	tax_rounding_rule,
	coterminate_day_month,
	primary_specialist_id,
	secondary_specialist_id,
	account_liable_flag,
	restriction_limit_amount,
	current_balance,
	password_text,
	high_priority_indicator,
	account_established_date,
	account_termination_date,
	account_activation_date,
	credit_classification_code,
	department,
	major_account_number,
	hotwatch_service_flag,
	hotwatch_svc_bal_ind,
	held_bill_expiration_date,
	hold_bill_flag,
	high_priority_remarks,
	po_effective_date,
	po_expiration_date,
	realtime_rate_flag,
	single_user_flag,
	watch_account_flag,
	watch_balance_indicator,
	geo_code,
	acct_life_cycle_status,
	account_name,
	deposit_refund_method,
	dormant_account_flag,
	npa_number,
	pin_number,
	suspension_date,
	write_off_adjustment_amount,
	write_off_payment_amount,
	write_off_amount,
	source_code,
	competitor_type,
	comments,
	dates_negative_tolerance,
	dates_positive_tolerance,
	date_type_preference,
	over_shipment_tolerance,
	under_shipment_tolerance,
	over_return_tolerance,
	under_return_tolerance,
	item_cross_ref_pref,
	ship_sets_include_lines_flag,
	arrivalsets_include_lines_flag,
	sched_date_push_flag,
	invoice_quantity_rule,
	pricing_event,
	account_replication_key,
	status_update_date,
	autopay_flag,
	notify_flag,
	last_batch_id,
	org_id,
	object_version_number,
	created_by_module,
	application_id,
	selling_party_id, 
	ADVANCE_PAYMENT_INDICATOR,
    DUNS_EXTENSION,
    FEDERAL_ENTITY_TYPE,
    TRADING_PARTNER_AGENCY_ID,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.HZ_CUST_ACCOUNTS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'hz_cust_accounts';
	
commit;