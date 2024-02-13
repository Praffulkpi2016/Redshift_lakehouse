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

drop TABLE if exists bec_ods.ap_suppliers;

CREATE TABLE IF NOT EXISTS bec_ods.ap_suppliers
(
	vendor_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,vendor_name VARCHAR(240)   ENCODE lzo
	,vendor_name_alt VARCHAR(320)   ENCODE lzo
	,segment1 VARCHAR(30)   ENCODE lzo
	,summary_flag VARCHAR(1)   ENCODE lzo
	,enabled_flag VARCHAR(1)   ENCODE lzo
	,segment2 VARCHAR(30)   ENCODE lzo
	,segment3 VARCHAR(30)   ENCODE lzo
	,segment4 VARCHAR(30)   ENCODE lzo
	,segment5 VARCHAR(30)   ENCODE lzo
	,last_update_login NUMERIC(15,0)  ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)  ENCODE az64
	,employee_id NUMERIC(15,0)  ENCODE az64
	,vendor_type_lookup_code VARCHAR(30)   ENCODE lzo
	,customer_num VARCHAR(25)   ENCODE lzo
	,one_time_flag VARCHAR(1)   ENCODE lzo
	,parent_vendor_id NUMERIC(15,0)   ENCODE az64
	,min_order_amount NUMERIC(28,10)   ENCODE az64
	,ship_to_location_id NUMERIC(15,0)  ENCODE az64
	,bill_to_location_id NUMERIC(15,0)   ENCODE az64
	,ship_via_lookup_code VARCHAR(25)   ENCODE lzo
	,freight_terms_lookup_code VARCHAR(25)   ENCODE lzo
	,fob_lookup_code VARCHAR(25)   ENCODE lzo
	,terms_id NUMERIC(15,0)   ENCODE az64
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,credit_status_lookup_code VARCHAR(25)   ENCODE lzo
	,credit_limit NUMERIC(28,10)   ENCODE az64
	,always_take_disc_flag VARCHAR(1)   ENCODE lzo
	,pay_date_basis_lookup_code VARCHAR(25)   ENCODE lzo
	,pay_group_lookup_code VARCHAR(25)   ENCODE lzo
	,payment_priority NUMERIC(15,0)   ENCODE az64
	,invoice_currency_code VARCHAR(15)   ENCODE lzo
	,payment_currency_code VARCHAR(15)   ENCODE lzo
	,invoice_amount_limit NUMERIC(28,10)   ENCODE az64
	,exchange_date_lookup_code VARCHAR(25)   ENCODE lzo
	,hold_all_payments_flag VARCHAR(1)   ENCODE lzo
	,hold_future_payments_flag VARCHAR(1)   ENCODE lzo
	,hold_reason VARCHAR(240)   ENCODE lzo
	,distribution_set_id NUMERIC(15,0)  ENCODE az64
	,accts_pay_code_combination_id NUMERIC(15,0)   ENCODE az64
	,disc_lost_code_combination_id NUMERIC(15,0)   ENCODE az64
	,disc_taken_code_combination_id NUMERIC(15,0)   ENCODE az64
	,expense_code_combination_id NUMERIC(15,0)   ENCODE az64
	,prepay_code_combination_id NUMERIC(15,0)   ENCODE az64
	,num_1099 VARCHAR(30)   ENCODE lzo
	,type_1099 VARCHAR(10)   ENCODE lzo
	,withholding_status_lookup_code VARCHAR(25)   ENCODE lzo
	,withholding_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,organization_type_lookup_code VARCHAR(25)   ENCODE lzo
	,vat_code VARCHAR(30)   ENCODE lzo
	,start_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date_active TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,minority_group_lookup_code VARCHAR(25)   ENCODE lzo
	,payment_method_lookup_code VARCHAR(25)   ENCODE lzo
	,bank_account_name VARCHAR(80)   ENCODE lzo
	,bank_account_num VARCHAR(30)   ENCODE lzo
	,bank_num VARCHAR(25)   ENCODE lzo
	,bank_account_type VARCHAR(25)   ENCODE lzo
	,women_owned_flag VARCHAR(1)   ENCODE lzo
	,small_business_flag VARCHAR(1)   ENCODE lzo
	,standard_industry_class VARCHAR(25)   ENCODE lzo
	,hold_flag VARCHAR(1)   ENCODE lzo
	,purchasing_hold_reason VARCHAR(240)   ENCODE lzo
	,hold_by NUMERIC(9,0)   ENCODE az64
	,hold_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,terms_date_basis VARCHAR(25)   ENCODE lzo
	,price_tolerance NUMERIC(28,10)   ENCODE az64
	,inspection_required_flag VARCHAR(1)   ENCODE lzo
	,receipt_required_flag VARCHAR(1)   ENCODE lzo
	,qty_rcv_tolerance NUMERIC(28,10)   ENCODE az64
	,qty_rcv_exception_code VARCHAR(25)   ENCODE lzo
	,enforce_ship_to_location_code VARCHAR(25)   ENCODE lzo
	,days_early_receipt_allowed NUMERIC(15,0)   ENCODE az64
	,days_late_receipt_allowed NUMERIC(15,0)   ENCODE az64
	,receipt_days_exception_code VARCHAR(25)   ENCODE lzo
	,receiving_routing_id NUMERIC(15,0)   ENCODE az64
	,allow_substitute_receipts_flag VARCHAR(1)   ENCODE lzo
	,allow_unordered_receipts_flag VARCHAR(1)   ENCODE lzo
	,hold_unmatched_invoices_flag VARCHAR(1)   ENCODE lzo
	,exclusive_payment_flag VARCHAR(1)   ENCODE lzo
	,ap_tax_rounding_rule VARCHAR(1)   ENCODE lzo
	,auto_tax_calc_flag VARCHAR(1)   ENCODE lzo
	,auto_tax_calc_override VARCHAR(1)   ENCODE lzo
	,amount_includes_tax_flag VARCHAR(1)   ENCODE lzo
	,tax_verification_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,name_control VARCHAR(4)   ENCODE lzo
	,state_reportable_flag VARCHAR(1)   ENCODE lzo
	,federal_reportable_flag VARCHAR(1)   ENCODE lzo
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
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)  ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,offset_vat_code VARCHAR(20)   ENCODE lzo
	,vat_registration_num VARCHAR(20)   ENCODE lzo
	,auto_calculate_interest_flag VARCHAR(1)   ENCODE lzo
	,validation_number NUMERIC(15,0)   ENCODE az64
	,exclude_freight_from_discount VARCHAR(1)   ENCODE lzo
	,tax_reporting_name VARCHAR(80)   ENCODE lzo
	,check_digits VARCHAR(30)   ENCODE lzo
	,bank_number VARCHAR(30)   ENCODE lzo
	,allow_awt_flag VARCHAR(1)   ENCODE lzo
	,awt_group_id NUMERIC(15,0)   ENCODE az64
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
	,edi_transaction_handling VARCHAR(25)   ENCODE lzo
	,edi_payment_method VARCHAR(25)   ENCODE lzo
	,edi_payment_format VARCHAR(25)   ENCODE lzo
	,edi_remittance_method VARCHAR(25)   ENCODE lzo
	,edi_remittance_instruction VARCHAR(256)   ENCODE lzo
	,bank_charge_bearer VARCHAR(1)   ENCODE lzo
	,bank_branch_type VARCHAR(25)   ENCODE lzo
	,match_option VARCHAR(25)   ENCODE lzo
	,future_dated_payment_ccid NUMERIC(15,0)   ENCODE az64
	,create_debit_memo_flag VARCHAR(25)   ENCODE lzo
	,offset_tax_flag VARCHAR(1)   ENCODE lzo
	,party_id NUMERIC(15,0)   ENCODE az64
	,parent_party_id NUMERIC(15,0)   ENCODE az64
	,ni_number VARCHAR(30)   ENCODE lzo
	,tca_sync_num_1099 VARCHAR(30)   ENCODE lzo
	,tca_sync_vendor_name VARCHAR(360)   ENCODE lzo
	,tca_sync_vat_reg_num VARCHAR(50)   ENCODE lzo
	,individual_1099 VARCHAR(30)   ENCODE lzo
	,unique_tax_reference_num NUMERIC(15,0)   ENCODE az64
	,partnership_utr NUMERIC(15,0)   ENCODE az64
	,partnership_name VARCHAR(240)   ENCODE lzo
	,cis_enabled_flag VARCHAR(1)   ENCODE lzo
	,first_name VARCHAR(240)   ENCODE lzo
	,second_name VARCHAR(240)   ENCODE lzo
	,last_name VARCHAR(240)   ENCODE lzo
	,salutation VARCHAR(30)   ENCODE lzo
	,trading_name VARCHAR(240)   ENCODE lzo
	,work_reference VARCHAR(30)   ENCODE lzo
	,company_registration_number VARCHAR(30)   ENCODE lzo
	,national_insurance_number VARCHAR(30)   ENCODE lzo
	,verification_number VARCHAR(30)   ENCODE lzo
	,verification_request_id NUMERIC(15,0)   ENCODE az64
	,match_status_flag VARCHAR(1)   ENCODE lzo
	,cis_verification_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,pay_awt_group_id NUMERIC(15,0)   ENCODE az64
	,cis_parent_vendor_id NUMERIC(15,0)   ENCODE az64
	,bus_class_last_certified_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,bus_class_last_certified_by NUMERIC(15,0)   ENCODE az64
    ,"VAT_REGISTRATION_NUM#1" VARCHAR(50)   ENCODE lzo
    ,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.ap_suppliers
(vendor_id,
	last_update_date,
	last_updated_by,
	vendor_name,
	vendor_name_alt,
	segment1,
	summary_flag,
	enabled_flag,
	segment2,
	segment3,
	segment4,
	segment5,
	last_update_login,
	creation_date,
	created_by,
	employee_id,
	vendor_type_lookup_code,
	customer_num,
	one_time_flag,
	parent_vendor_id,
	min_order_amount,
	ship_to_location_id,
	bill_to_location_id,
	ship_via_lookup_code,
	freight_terms_lookup_code,
	fob_lookup_code,
	terms_id,
	set_of_books_id,
	credit_status_lookup_code,
	credit_limit,
	always_take_disc_flag,
	pay_date_basis_lookup_code,
	pay_group_lookup_code,
	payment_priority,
	invoice_currency_code,
	payment_currency_code,
	invoice_amount_limit,
	exchange_date_lookup_code,
	hold_all_payments_flag,
	hold_future_payments_flag,
	hold_reason,
	distribution_set_id,
	accts_pay_code_combination_id,
	disc_lost_code_combination_id,
	disc_taken_code_combination_id,
	expense_code_combination_id,
	prepay_code_combination_id,
	num_1099,
	type_1099,
	withholding_status_lookup_code,
	withholding_start_date,
	organization_type_lookup_code,
	vat_code,
	start_date_active,
	end_date_active,
	minority_group_lookup_code,
	payment_method_lookup_code,
	bank_account_name,
	bank_account_num,
	bank_num,
	bank_account_type,
	women_owned_flag,
	small_business_flag,
	standard_industry_class,
	hold_flag,
	purchasing_hold_reason,
	hold_by,
	hold_date,
	terms_date_basis,
	price_tolerance,
	inspection_required_flag,
	receipt_required_flag,
	qty_rcv_tolerance,
	qty_rcv_exception_code,
	enforce_ship_to_location_code,
	days_early_receipt_allowed,
	days_late_receipt_allowed,
	receipt_days_exception_code,
	receiving_routing_id,
	allow_substitute_receipts_flag,
	allow_unordered_receipts_flag,
	hold_unmatched_invoices_flag,
	exclusive_payment_flag,
	ap_tax_rounding_rule,
	auto_tax_calc_flag,
	auto_tax_calc_override,
	amount_includes_tax_flag,
	tax_verification_date,
	name_control,
	state_reportable_flag,
	federal_reportable_flag,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	offset_vat_code,
	vat_registration_num,
	auto_calculate_interest_flag,
	validation_number,
	exclude_freight_from_discount,
	tax_reporting_name,
	check_digits,
	bank_number,
	allow_awt_flag,
	awt_group_id,
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
	edi_transaction_handling,
	edi_payment_method,
	edi_payment_format,
	edi_remittance_method,
	edi_remittance_instruction,
	bank_charge_bearer,
	bank_branch_type,
	match_option,
	future_dated_payment_ccid,
	create_debit_memo_flag,
	offset_tax_flag,
	party_id,
	parent_party_id,
	ni_number,
	tca_sync_num_1099,
	tca_sync_vendor_name,
	tca_sync_vat_reg_num,
	individual_1099,
	unique_tax_reference_num,
	partnership_utr,
	partnership_name,
	cis_enabled_flag,
	first_name,
	second_name,
	last_name,
	salutation,
	trading_name,
	work_reference,
	company_registration_number,
	national_insurance_number,
	verification_number,
	verification_request_id,
	match_status_flag,
	cis_verification_date,
	pay_awt_group_id,
	cis_parent_vendor_id,
	bus_class_last_certified_date,
	bus_class_last_certified_by,
	"VAT_REGISTRATION_NUM#1",
	KCA_OPERATION,
    IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(select
	vendor_id,
	last_update_date,
	last_updated_by,
	vendor_name,
	vendor_name_alt,
	segment1,
	summary_flag,
	enabled_flag,
	segment2,
	segment3,
	segment4,
	segment5,
	last_update_login,
	creation_date,
	created_by,
	employee_id,
	vendor_type_lookup_code,
	customer_num,
	one_time_flag,
	parent_vendor_id,
	min_order_amount,
	ship_to_location_id,
	bill_to_location_id,
	ship_via_lookup_code,
	freight_terms_lookup_code,
	fob_lookup_code,
	terms_id,
	set_of_books_id,
	credit_status_lookup_code,
	credit_limit,
	always_take_disc_flag,
	pay_date_basis_lookup_code,
	pay_group_lookup_code,
	payment_priority,
	invoice_currency_code,
	payment_currency_code,
	invoice_amount_limit,
	exchange_date_lookup_code,
	hold_all_payments_flag,
	hold_future_payments_flag,
	hold_reason,
	distribution_set_id,
	accts_pay_code_combination_id,
	disc_lost_code_combination_id,
	disc_taken_code_combination_id,
	expense_code_combination_id,
	prepay_code_combination_id,
	num_1099,
	type_1099,
	withholding_status_lookup_code,
	withholding_start_date,
	organization_type_lookup_code,
	vat_code,
	start_date_active,
	end_date_active,
	minority_group_lookup_code,
	payment_method_lookup_code,
	bank_account_name,
	bank_account_num,
	bank_num,
	bank_account_type,
	women_owned_flag,
	small_business_flag,
	standard_industry_class,
	hold_flag,
	purchasing_hold_reason,
	hold_by,
	hold_date,
	terms_date_basis,
	price_tolerance,
	inspection_required_flag,
	receipt_required_flag,
	qty_rcv_tolerance,
	qty_rcv_exception_code,
	enforce_ship_to_location_code,
	days_early_receipt_allowed,
	days_late_receipt_allowed,
	receipt_days_exception_code,
	receiving_routing_id,
	allow_substitute_receipts_flag,
	allow_unordered_receipts_flag,
	hold_unmatched_invoices_flag,
	exclusive_payment_flag,
	ap_tax_rounding_rule,
	auto_tax_calc_flag,
	auto_tax_calc_override,
	amount_includes_tax_flag,
	tax_verification_date,
	name_control,
	state_reportable_flag,
	federal_reportable_flag,
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
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	offset_vat_code,
	vat_registration_num,
	auto_calculate_interest_flag,
	validation_number,
	exclude_freight_from_discount,
	tax_reporting_name,
	check_digits,
	bank_number,
	allow_awt_flag,
	awt_group_id,
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
	edi_transaction_handling,
	edi_payment_method,
	edi_payment_format,
	edi_remittance_method,
	edi_remittance_instruction,
	bank_charge_bearer,
	bank_branch_type,
	match_option,
	future_dated_payment_ccid,
	create_debit_memo_flag,
	offset_tax_flag,
	party_id,
	parent_party_id,
	ni_number,
	tca_sync_num_1099,
	tca_sync_vendor_name,
	tca_sync_vat_reg_num,
	individual_1099,
	unique_tax_reference_num,
	partnership_utr,
	partnership_name,
	cis_enabled_flag,
	first_name,
	second_name,
	last_name,
	salutation,
	trading_name,
	work_reference,
	company_registration_number,
	national_insurance_number,
	verification_number,
	verification_request_id,
	match_status_flag,
	cis_verification_date,
	pay_awt_group_id,
	cis_parent_vendor_id,
	bus_class_last_certified_date,
	bus_class_last_certified_by,
	"VAT_REGISTRATION_NUM#1",
    KCA_OPERATION,
    'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from
	bec_ods_stg.ap_suppliers);

end;

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_suppliers'; 
	
commit;