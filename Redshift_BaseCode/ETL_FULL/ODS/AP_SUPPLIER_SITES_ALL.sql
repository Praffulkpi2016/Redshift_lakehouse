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
drop table if exists bec_ods.ap_supplier_sites_all;

CREATE TABLE IF NOT EXISTS bec_ods.ap_supplier_sites_all
(
	vendor_site_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,vendor_site_code VARCHAR(15)   ENCODE lzo
	,vendor_site_code_alt VARCHAR(320)   ENCODE lzo
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)  ENCODE az64
	,purchasing_site_flag VARCHAR(1)   ENCODE lzo
	,rfq_only_site_flag VARCHAR(1)   ENCODE lzo
	,pay_site_flag VARCHAR(1)   ENCODE lzo
	,attention_ar_flag VARCHAR(1)   ENCODE lzo
	,address_line1 VARCHAR(240)   ENCODE lzo
	,address_lines_alt VARCHAR(560)   ENCODE lzo
	,address_line2 VARCHAR(240)   ENCODE lzo
	,address_line3 VARCHAR(240)   ENCODE lzo
	,city VARCHAR(60)   ENCODE lzo
	,state VARCHAR(150)   ENCODE lzo
	,zip VARCHAR(60)   ENCODE lzo
	,province VARCHAR(150)   ENCODE lzo
	,country VARCHAR(60)   ENCODE lzo
	,area_code VARCHAR(10)   ENCODE lzo
	,phone VARCHAR(15)   ENCODE lzo
	,customer_num VARCHAR(25)   ENCODE lzo
	,ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,bill_to_location_id NUMERIC(15,0)   ENCODE az64
	,ship_via_lookup_code VARCHAR(25)   ENCODE lzo
	,freight_terms_lookup_code VARCHAR(25)   ENCODE lzo
	,fob_lookup_code VARCHAR(25)   ENCODE lzo
	,inactive_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,fax VARCHAR(15)   ENCODE lzo
	,fax_area_code VARCHAR(10)   ENCODE lzo
	,telex VARCHAR(15)   ENCODE lzo
	,payment_method_lookup_code VARCHAR(25)   ENCODE lzo
	,bank_account_name VARCHAR(80)   ENCODE lzo
	,bank_account_num VARCHAR(30)   ENCODE lzo
	,bank_num VARCHAR(25)   ENCODE lzo
	,bank_account_type VARCHAR(25)   ENCODE lzo
	,terms_date_basis VARCHAR(25)   ENCODE lzo
	,current_catalog_num VARCHAR(20)   ENCODE lzo
	,vat_code VARCHAR(30)   ENCODE lzo
	,distribution_set_id NUMERIC(15,0)   ENCODE az64
	,accts_pay_code_combination_id NUMERIC(15,0)   ENCODE az64
	,prepay_code_combination_id NUMERIC(15,0)   ENCODE az64
	,pay_group_lookup_code VARCHAR(25)   ENCODE lzo
	,payment_priority NUMERIC(15,0)   ENCODE az64
	,terms_id NUMERIC(15,0)   ENCODE az64
	,invoice_amount_limit NUMERIC(28,10)   ENCODE az64
	,pay_date_basis_lookup_code VARCHAR(25)   ENCODE lzo
	,always_take_disc_flag VARCHAR(1)   ENCODE lzo
	,invoice_currency_code VARCHAR(15)   ENCODE lzo
	,payment_currency_code VARCHAR(15)   ENCODE lzo
	,hold_all_payments_flag VARCHAR(1)   ENCODE lzo
	,hold_future_payments_flag VARCHAR(1)   ENCODE lzo
	,hold_reason VARCHAR(240)   ENCODE lzo
	,hold_unmatched_invoices_flag VARCHAR(1)   ENCODE lzo
	,ap_tax_rounding_rule VARCHAR(1)   ENCODE lzo
	,auto_tax_calc_flag VARCHAR(1)   ENCODE lzo
	,auto_tax_calc_override VARCHAR(1)   ENCODE lzo
	,amount_includes_tax_flag VARCHAR(1)   ENCODE lzo
	,exclusive_payment_flag VARCHAR(1)   ENCODE lzo
	,tax_reporting_site_flag VARCHAR(1)   ENCODE lzo
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
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,validation_number NUMERIC(15,0)   ENCODE az64
	,exclude_freight_from_discount VARCHAR(1)   ENCODE lzo
	,vat_registration_num VARCHAR(20)   ENCODE lzo
	,offset_vat_code VARCHAR(20)   ENCODE lzo
	,org_id NUMERIC(15,0)  ENCODE az64
	,check_digits VARCHAR(30)   ENCODE lzo
	,bank_number VARCHAR(30)   ENCODE lzo
	,address_line4 VARCHAR(240)   ENCODE lzo
	,county VARCHAR(150)   ENCODE lzo
	,address_style VARCHAR(30)   ENCODE lzo
	,language VARCHAR(30)   ENCODE lzo
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
	,edi_id_number VARCHAR(30)   ENCODE lzo
	,edi_payment_method VARCHAR(25)   ENCODE lzo
	,edi_payment_format VARCHAR(25)   ENCODE lzo
	,edi_remittance_method VARCHAR(25)   ENCODE lzo
	,bank_charge_bearer VARCHAR(1)   ENCODE lzo
	,edi_remittance_instruction VARCHAR(256)   ENCODE lzo
	,bank_branch_type VARCHAR(25)   ENCODE lzo
	,pay_on_code VARCHAR(25)   ENCODE lzo
	,default_pay_site_id NUMERIC(15,0)   ENCODE az64
	,pay_on_receipt_summary_code VARCHAR(25)   ENCODE lzo
	,tp_header_id NUMERIC(15,0)   ENCODE az64
	,ece_tp_location_code VARCHAR(60)   ENCODE lzo
	,pcard_site_flag VARCHAR(1)   ENCODE lzo
	,match_option VARCHAR(25)   ENCODE lzo
	,country_of_origin_code VARCHAR(2)   ENCODE lzo
	,future_dated_payment_ccid NUMERIC(15,0)   ENCODE az64
	,create_debit_memo_flag VARCHAR(25)   ENCODE lzo
	,offset_tax_flag VARCHAR(1)   ENCODE lzo
	,supplier_notif_method VARCHAR(25)   ENCODE lzo
	,email_address VARCHAR(2000)   ENCODE lzo
	,remittance_email VARCHAR(2000)   ENCODE lzo
	,primary_pay_site_flag VARCHAR(1)   ENCODE lzo
	,shipping_control VARCHAR(30)   ENCODE lzo
	,selling_company_identifier VARCHAR(10)   ENCODE lzo
	,gapless_inv_num_flag VARCHAR(1)   ENCODE lzo
	,duns_number VARCHAR(30)   ENCODE lzo
	,tolerance_id NUMERIC(15,0)   ENCODE az64
	,location_id NUMERIC(15,0)   ENCODE az64
	,party_site_id NUMERIC(15,0)   ENCODE az64
	,services_tolerance_id NUMERIC(15,0)   ENCODE az64
	,retainage_rate NUMERIC(28,10)   ENCODE az64
	,tca_sync_state VARCHAR(150)   ENCODE lzo
	,tca_sync_province VARCHAR(150)   ENCODE lzo
	,tca_sync_county VARCHAR(150)   ENCODE lzo
	,tca_sync_city VARCHAR(60)   ENCODE lzo
	,tca_sync_zip VARCHAR(60)   ENCODE lzo
	,tca_sync_country VARCHAR(60)   ENCODE lzo
	,pay_awt_group_id NUMERIC(15,0)   ENCODE az64
	,CAGE_CODE VARCHAR(5)   ENCODE lzo	
    ,LEGAL_BUSINESS_NAME VARCHAR(240)   ENCODE lzo
    ,DOING_BUS_AS_NAME VARCHAR(240) ENCODE lzo
	,DIVISION_NAME VARCHAR(60)   ENCODE lzo	
    ,SMALL_BUSINESS_CODE VARCHAR(10)   ENCODE lzo	
    ,CCR_COMMENTS VARCHAR(240)   ENCODE lzo
	,DEBARMENT_START_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,DEBARMENT_END_DATE TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,ACK_LEAD_TIME NUMERIC(28,10)   ENCODE az64
	,"VAT_REGISTRATION_NUM#1" VARCHAR(50)   ENCODE lzo	
    ,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;
insert into bec_ods.ap_supplier_sites_all
(vendor_site_id,
	last_update_date,
	last_updated_by,
	vendor_id,
	vendor_site_code,
	vendor_site_code_alt,
	last_update_login,
	creation_date,
	created_by,
	purchasing_site_flag,
	rfq_only_site_flag,
	pay_site_flag,
	attention_ar_flag,
	address_line1,
	address_lines_alt,
	address_line2,
	address_line3,
	city,
	state,
	zip,
	province,
	country,
	area_code,
	phone,
	customer_num,
	ship_to_location_id,
	bill_to_location_id,
	ship_via_lookup_code,
	freight_terms_lookup_code,
	fob_lookup_code,
	inactive_date,
	fax,
	fax_area_code,
	telex,
	payment_method_lookup_code,
	bank_account_name,
	bank_account_num,
	bank_num,
	bank_account_type,
	terms_date_basis,
	current_catalog_num,
	vat_code,
	distribution_set_id,
	accts_pay_code_combination_id,
	prepay_code_combination_id,
	pay_group_lookup_code,
	payment_priority,
	terms_id,
	invoice_amount_limit,
	pay_date_basis_lookup_code,
	always_take_disc_flag,
	invoice_currency_code,
	payment_currency_code,
	hold_all_payments_flag,
	hold_future_payments_flag,
	hold_reason,
	hold_unmatched_invoices_flag,
	ap_tax_rounding_rule,
	auto_tax_calc_flag,
	auto_tax_calc_override,
	amount_includes_tax_flag,
	exclusive_payment_flag,
	tax_reporting_site_flag,
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
	validation_number,
	exclude_freight_from_discount,
	vat_registration_num,
	offset_vat_code,
	org_id,
	check_digits,
	bank_number,
	address_line4,
	county,
	address_style,
	language,
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
	edi_id_number,
	edi_payment_method,
	edi_payment_format,
	edi_remittance_method,
	bank_charge_bearer,
	edi_remittance_instruction,
	bank_branch_type,
	pay_on_code,
	default_pay_site_id,
	pay_on_receipt_summary_code,
	tp_header_id,
	ece_tp_location_code,
	pcard_site_flag,
	match_option,
	country_of_origin_code,
	future_dated_payment_ccid,
	create_debit_memo_flag,
	offset_tax_flag,
	supplier_notif_method,
	email_address,
	remittance_email,
	primary_pay_site_flag,
	shipping_control,
	selling_company_identifier,
	gapless_inv_num_flag,
	duns_number,
	tolerance_id,
	location_id,
	party_site_id,
	services_tolerance_id,
	retainage_rate,
	tca_sync_state,
	tca_sync_province,
	tca_sync_county,
	tca_sync_city,
	tca_sync_zip,
	tca_sync_country,
	pay_awt_group_id,
	cage_code,
	legal_business_name,
	doing_bus_as_name,
	division_name,
	small_business_code,
	ccr_comments,
	debarment_start_date,
	debarment_end_date,
	ack_lead_time,
	"vat_registration_num#1",
	KCA_OPERATION,
    IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(select
	vendor_site_id,
	last_update_date,
	last_updated_by,
	vendor_id,
	vendor_site_code,
	vendor_site_code_alt,
	last_update_login,
	creation_date,
	created_by,
	purchasing_site_flag,
	rfq_only_site_flag,
	pay_site_flag,
	attention_ar_flag,
	address_line1,
	address_lines_alt,
	address_line2,
	address_line3,
	city,
	state,
	zip,
	province,
	country,
	area_code,
	phone,
	customer_num,
	ship_to_location_id,
	bill_to_location_id,
	ship_via_lookup_code,
	freight_terms_lookup_code,
	fob_lookup_code,
	inactive_date,
	fax,
	fax_area_code,
	telex,
	payment_method_lookup_code,
	bank_account_name,
	bank_account_num,
	bank_num,
	bank_account_type,
	terms_date_basis,
	current_catalog_num,
	vat_code,
	distribution_set_id,
	accts_pay_code_combination_id,
	prepay_code_combination_id,
	pay_group_lookup_code,
	payment_priority,
	terms_id,
	invoice_amount_limit,
	pay_date_basis_lookup_code,
	always_take_disc_flag,
	invoice_currency_code,
	payment_currency_code,
	hold_all_payments_flag,
	hold_future_payments_flag,
	hold_reason,
	hold_unmatched_invoices_flag,
	ap_tax_rounding_rule,
	auto_tax_calc_flag,
	auto_tax_calc_override,
	amount_includes_tax_flag,
	exclusive_payment_flag,
	tax_reporting_site_flag,
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
	validation_number,
	exclude_freight_from_discount,
	vat_registration_num,
	offset_vat_code,
	org_id,
	check_digits,
	bank_number,
	address_line4,
	county,
	address_style,
	language,
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
	edi_id_number,
	edi_payment_method,
	edi_payment_format,
	edi_remittance_method,
	bank_charge_bearer,
	edi_remittance_instruction,
	bank_branch_type,
	pay_on_code,
	default_pay_site_id,
	pay_on_receipt_summary_code,
	tp_header_id,
	ece_tp_location_code,
	pcard_site_flag,
	match_option,
	country_of_origin_code,
	future_dated_payment_ccid,
	create_debit_memo_flag,
	offset_tax_flag,
	supplier_notif_method,
	email_address,
	remittance_email,
	primary_pay_site_flag,
	shipping_control,
	selling_company_identifier,
	gapless_inv_num_flag,
	duns_number,
	tolerance_id,
	location_id,
	party_site_id,
	services_tolerance_id,
	retainage_rate,
	tca_sync_state,
	tca_sync_province,
	tca_sync_county,
	tca_sync_city,
	tca_sync_zip,
	tca_sync_country,
	pay_awt_group_id,
	CAGE_CODE,
	LEGAL_BUSINESS_NAME,
	DOING_BUS_AS_NAME,
	DIVISION_NAME,
	SMALL_BUSINESS_CODE,
	CCR_COMMENTS,
	DEBARMENT_START_DATE,
	DEBARMENT_END_DATE,
	ACK_LEAD_TIME,
	"VAT_REGISTRATION_NUM#1",
    KCA_OPERATION,
    'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from
	bec_ods_stg.ap_supplier_sites_all);
end;
 

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_supplier_sites_all';
	
commit;
 