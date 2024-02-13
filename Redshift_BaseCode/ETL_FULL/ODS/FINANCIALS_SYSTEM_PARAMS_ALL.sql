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

DROP TABLE if exists bec_ods.FINANCIALS_SYSTEM_PARAMS_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.FINANCIALS_SYSTEM_PARAMS_ALL
(
	last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,payment_method_lookup_code VARCHAR(25)   ENCODE lzo
	,user_defined_vendor_num_code VARCHAR(25)   ENCODE lzo
	,vendor_num_start_num NUMERIC(28,10)   ENCODE az64
	,ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,bill_to_location_id NUMERIC(15,0)   ENCODE az64
	,ship_via_lookup_code VARCHAR(25)   ENCODE lzo
	,fob_lookup_code VARCHAR(25)   ENCODE lzo
	,terms_id NUMERIC(15,0)   ENCODE az64
	,always_take_disc_flag VARCHAR(1)   ENCODE lzo
	,pay_date_basis_lookup_code VARCHAR(25)   ENCODE lzo
	,invoice_currency_code VARCHAR(15)   ENCODE lzo
	,payment_currency_code VARCHAR(15)   ENCODE lzo
	,accts_pay_code_combination_id NUMERIC(15,0)   ENCODE az64
	,prepay_code_combination_id NUMERIC(15,0)   ENCODE az64
	,disc_taken_code_combination_id NUMERIC(15,0)   ENCODE az64
	,future_period_limit NUMERIC(3,0)   ENCODE az64
	,reserve_at_completion_flag VARCHAR(1)   ENCODE lzo
	,res_encumb_code_combination_id NUMERIC(15,0)   ENCODE az64
	,req_encumbrance_flag VARCHAR(1)   ENCODE lzo
	,req_encumbrance_type_id NUMERIC(15,0)   ENCODE az64
	,purch_encumbrance_flag VARCHAR(1)   ENCODE lzo
	,purch_encumbrance_type_id NUMERIC(15,0)   ENCODE az64
	,inv_encumbrance_type_id NUMERIC(15,0)   ENCODE az64
	,manual_vendor_num_type VARCHAR(25)   ENCODE lzo
	,inventory_organization_id NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,freight_terms_lookup_code VARCHAR(25)   ENCODE lzo
	,rfq_only_site_flag VARCHAR(1)   ENCODE lzo
	,receipt_acceptance_days NUMERIC(15,0)   ENCODE az64
	,business_group_id NUMERIC(15,0)   ENCODE az64
	,expense_check_address_flag VARCHAR(30)   ENCODE lzo
	,terms_date_basis VARCHAR(25)   ENCODE lzo
	,use_positions_flag VARCHAR(1)   ENCODE lzo
	,rate_var_code_combination_id NUMERIC(15,0)   ENCODE az64
	,hold_unmatched_invoices_flag VARCHAR(1)   ENCODE lzo
	,exclusive_payment_flag VARCHAR(1)   ENCODE lzo
	,revision_sort_ordering NUMERIC(28,10)   ENCODE az64
	,vat_registration_num VARCHAR(20)   ENCODE lzo
	,vat_country_code VARCHAR(15)   ENCODE lzo
	,rate_var_gain_ccid NUMERIC(15,0)   ENCODE az64
	,rate_var_loss_ccid NUMERIC(15,0)   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,bank_charge_bearer VARCHAR(1)   ENCODE lzo
	,vat_code VARCHAR(15)   ENCODE lzo
	,match_option VARCHAR(25)   ENCODE lzo
	,non_recoverable_tax_flag VARCHAR(1)   ENCODE lzo
	,tax_rounding_rule VARCHAR(30)   ENCODE lzo
	,"precision" NUMERIC(1,0)  ENCODE az64
	,minimum_accountable_unit NUMERIC(28,10)   ENCODE az64
	,default_recovery_rate NUMERIC(28,10)   ENCODE az64
	,cash_basis_enc_nr_tax VARCHAR(30)   ENCODE lzo
	,future_dated_payment_ccid NUMERIC(15,0)   ENCODE az64
	,expense_clearing_ccid NUMERIC(15,0)   ENCODE az64
	,misc_charge_ccid NUMERIC(15,0)   ENCODE az64
	,global_attribute_category VARCHAR(150)   ENCODE lzo
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
	,retainage_code_combination_id NUMERIC(15,0)   ENCODE az64 
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.FINANCIALS_SYSTEM_PARAMS_ALL (
    last_update_date,
	last_updated_by,
	set_of_books_id,
	payment_method_lookup_code,
	user_defined_vendor_num_code,
	vendor_num_start_num,
	ship_to_location_id,
	bill_to_location_id,
	ship_via_lookup_code,
	fob_lookup_code,
	terms_id,
	always_take_disc_flag,
	pay_date_basis_lookup_code,
	invoice_currency_code,
	payment_currency_code,
	accts_pay_code_combination_id,
	prepay_code_combination_id,
	disc_taken_code_combination_id,
	future_period_limit,
	reserve_at_completion_flag,
	res_encumb_code_combination_id,
	req_encumbrance_flag,
	req_encumbrance_type_id,
	purch_encumbrance_flag,
	purch_encumbrance_type_id,
	inv_encumbrance_type_id,
	manual_vendor_num_type,
	inventory_organization_id,
	last_update_login,
	creation_date,
	created_by,
	freight_terms_lookup_code,
	rfq_only_site_flag,
	receipt_acceptance_days,
	business_group_id,
	expense_check_address_flag,
	terms_date_basis,
	use_positions_flag,
	rate_var_code_combination_id,
	hold_unmatched_invoices_flag,
	exclusive_payment_flag,
	revision_sort_ordering,
	vat_registration_num,
	vat_country_code,
	rate_var_gain_ccid,
	rate_var_loss_ccid,
	org_id,
	bank_charge_bearer,
	vat_code,
	match_option,
	non_recoverable_tax_flag,
	tax_rounding_rule,
	"precision",
	minimum_accountable_unit,
	default_recovery_rate,
	cash_basis_enc_nr_tax,
	future_dated_payment_ccid,
	expense_clearing_ccid,
	misc_charge_ccid,
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
	retainage_code_combination_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        last_update_date,
	last_updated_by,
	set_of_books_id,
	payment_method_lookup_code,
	user_defined_vendor_num_code,
	vendor_num_start_num,
	ship_to_location_id,
	bill_to_location_id,
	ship_via_lookup_code,
	fob_lookup_code,
	terms_id,
	always_take_disc_flag,
	pay_date_basis_lookup_code,
	invoice_currency_code,
	payment_currency_code,
	accts_pay_code_combination_id,
	prepay_code_combination_id,
	disc_taken_code_combination_id,
	future_period_limit,
	reserve_at_completion_flag,
	res_encumb_code_combination_id,
	req_encumbrance_flag,
	req_encumbrance_type_id,
	purch_encumbrance_flag,
	purch_encumbrance_type_id,
	inv_encumbrance_type_id,
	manual_vendor_num_type,
	inventory_organization_id,
	last_update_login,
	creation_date,
	created_by,
	freight_terms_lookup_code,
	rfq_only_site_flag,
	receipt_acceptance_days,
	business_group_id,
	expense_check_address_flag,
	terms_date_basis,
	use_positions_flag,
	rate_var_code_combination_id,
	hold_unmatched_invoices_flag,
	exclusive_payment_flag,
	revision_sort_ordering,
	vat_registration_num,
	vat_country_code,
	rate_var_gain_ccid,
	rate_var_loss_ccid,
	org_id,
	bank_charge_bearer,
	vat_code,
	match_option,
	non_recoverable_tax_flag,
	tax_rounding_rule,
	"precision",
	minimum_accountable_unit,
	default_recovery_rate,
	cash_basis_enc_nr_tax,
	future_dated_payment_ccid,
	expense_clearing_ccid,
	misc_charge_ccid,
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
	retainage_code_combination_id,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.FINANCIALS_SYSTEM_PARAMS_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'financials_system_params_all';
	
commit;