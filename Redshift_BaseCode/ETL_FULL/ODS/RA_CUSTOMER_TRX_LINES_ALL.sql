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

DROP TABLE if exists bec_ods.RA_CUSTOMER_TRX_LINES_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.RA_CUSTOMER_TRX_LINES_ALL
(
	customer_trx_line_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,customer_trx_id NUMERIC(15,0)   ENCODE az64
	,line_number NUMERIC(15,0)   ENCODE az64
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,reason_code VARCHAR(30)   ENCODE lzo
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
	,previous_customer_trx_id NUMERIC(15,0)   ENCODE az64
	,previous_customer_trx_line_id NUMERIC(15,0)   ENCODE az64
	,quantity_ordered NUMERIC(28,10)   ENCODE az64
	,quantity_credited NUMERIC(28,10)   ENCODE az64
	,quantity_invoiced NUMERIC(28,10)   ENCODE az64
	,unit_standard_price NUMERIC(28,10)   ENCODE az64
	,unit_selling_price NUMERIC(28,10)   ENCODE az64
	,sales_order VARCHAR(50)   ENCODE lzo
	,sales_order_revision NUMERIC(28,10)   ENCODE az64
	,sales_order_line VARCHAR(30)   ENCODE lzo
	,sales_order_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,accounting_rule_id NUMERIC(15,0)   ENCODE az64
	,accounting_rule_duration NUMERIC(15,0)   ENCODE az64
	,line_type VARCHAR(20)   ENCODE lzo
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
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,rule_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,initial_customer_trx_line_id NUMERIC(15,0)   ENCODE az64
	,interface_line_context VARCHAR(30)   ENCODE lzo
	,interface_line_attribute1 VARCHAR(150)   ENCODE lzo
	,interface_line_attribute2 VARCHAR(150)   ENCODE lzo
	,interface_line_attribute3 VARCHAR(150)   ENCODE lzo
	,interface_line_attribute4 VARCHAR(150)   ENCODE lzo
	,interface_line_attribute5 VARCHAR(150)   ENCODE lzo
	,interface_line_attribute6 VARCHAR(150)   ENCODE lzo
	,interface_line_attribute7 VARCHAR(150)   ENCODE lzo
	,interface_line_attribute8 VARCHAR(150)   ENCODE lzo
	,sales_order_source VARCHAR(50)   ENCODE lzo
	,taxable_flag VARCHAR(1)   ENCODE lzo
	,extended_amount NUMERIC(28,10)   ENCODE az64
	,revenue_amount NUMERIC(28,10)   ENCODE az64
	,autorule_complete_flag VARCHAR(1)   ENCODE lzo
	,link_to_cust_trx_line_id NUMERIC(15,0)   ENCODE az64
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,tax_precedence NUMERIC(28,10)   ENCODE az64
	,tax_rate NUMERIC(28,10)   ENCODE az64
	,item_exception_rate_id NUMERIC(15,0)   ENCODE az64
	,tax_exemption_id NUMERIC(15,0)   ENCODE az64
	,memo_line_id NUMERIC(15,0)   ENCODE az64
	,autorule_duration_processed NUMERIC(15,0)   ENCODE az64
	,uom_code VARCHAR(3)   ENCODE lzo
	,default_ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,default_ussgl_trx_code_context VARCHAR(30)   ENCODE lzo
	,interface_line_attribute10 VARCHAR(150)   ENCODE lzo
	,interface_line_attribute11 VARCHAR(150)   ENCODE lzo
	,interface_line_attribute12 VARCHAR(150)   ENCODE lzo
	,interface_line_attribute13 VARCHAR(150)   ENCODE lzo
	,interface_line_attribute14 VARCHAR(150)   ENCODE lzo
	,interface_line_attribute15 VARCHAR(150)   ENCODE lzo
	,interface_line_attribute9 VARCHAR(150)   ENCODE lzo
	,vat_tax_id NUMERIC(15,0)   ENCODE az64
	,autotax VARCHAR(1)   ENCODE lzo
	,last_period_to_credit NUMERIC(28,10)   ENCODE az64
	,item_context VARCHAR(30)   ENCODE lzo
	,tax_exempt_flag VARCHAR(1)   ENCODE lzo
	,tax_exempt_number VARCHAR(80)   ENCODE lzo
	,tax_exempt_reason_code VARCHAR(30)   ENCODE lzo
	,tax_vendor_return_code VARCHAR(30)   ENCODE lzo
	,sales_tax_id NUMERIC(15,0)   ENCODE az64
	,location_segment_id NUMERIC(15,0)   ENCODE az64
	,movement_id NUMERIC(15,0)   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,wh_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,gross_unit_selling_price NUMERIC(28,10)   ENCODE az64
	,gross_extended_amount NUMERIC(28,10)   ENCODE az64
	,amount_includes_tax_flag VARCHAR(1)   ENCODE lzo
	,taxable_amount NUMERIC(28,10)   ENCODE az64
	,warehouse_id NUMERIC(15,0)   ENCODE az64
	,translated_description VARCHAR(1000)   ENCODE lzo
	,extended_acctd_amount NUMERIC(28,10)   ENCODE az64
	,br_ref_customer_trx_id NUMERIC(15,0)   ENCODE az64
	,br_ref_payment_schedule_id NUMERIC(15,0)   ENCODE az64
	,br_adjustment_id NUMERIC(15,0)   ENCODE az64
	,mrc_extended_acctd_amount VARCHAR(2000)   ENCODE lzo
	,payment_set_id NUMERIC(15,0)   ENCODE az64
	,contract_line_id VARCHAR(50)   ENCODE lzo
	,source_data_key1 VARCHAR(150)   ENCODE lzo
	,source_data_key2 VARCHAR(150)   ENCODE lzo
	,source_data_key3 VARCHAR(150)   ENCODE lzo
	,source_data_key4 VARCHAR(150)   ENCODE lzo
	,source_data_key5 VARCHAR(150)   ENCODE lzo
	,invoiced_line_acctg_level VARCHAR(15)   ENCODE lzo
	,override_auto_accounting_flag VARCHAR(1)   ENCODE lzo
	,ship_to_customer_id NUMERIC(15,0)   ENCODE az64
	,ship_to_address_id NUMERIC(15,0)   ENCODE az64
	,ship_to_site_use_id NUMERIC(15,0)   ENCODE az64
	,ship_to_contact_id NUMERIC(15,0)   ENCODE az64
	,historical_flag VARCHAR(1)   ENCODE lzo
	,tax_line_id NUMERIC(15,0)   ENCODE az64
	,line_recoverable NUMERIC(28,10)   ENCODE az64
	,tax_recoverable NUMERIC(28,10)   ENCODE az64
	,tax_classification_code VARCHAR(50)   ENCODE lzo
	,amount_due_remaining NUMERIC(28,10)   ENCODE az64
	,acctd_amount_due_remaining NUMERIC(28,10)   ENCODE az64
	,amount_due_original NUMERIC(28,10)   ENCODE az64
	,acctd_amount_due_original NUMERIC(28,10)   ENCODE az64
	,chrg_amount_remaining NUMERIC(28,10)   ENCODE az64
	,chrg_acctd_amount_remaining NUMERIC(28,10)   ENCODE az64
	,frt_adj_remaining NUMERIC(28,10)   ENCODE az64
	,frt_adj_acctd_remaining NUMERIC(28,10)   ENCODE az64
	,frt_ed_amount NUMERIC(28,10)   ENCODE az64
	,frt_ed_acctd_amount NUMERIC(28,10)   ENCODE az64
	,frt_uned_amount NUMERIC(28,10)   ENCODE az64
	,frt_uned_acctd_amount NUMERIC(28,10)   ENCODE az64
	,deferral_exclusion_flag VARCHAR(1)   ENCODE lzo
	,rule_end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,payment_trxn_extension_id NUMERIC(15,0)   ENCODE az64
	,interest_line_id NUMERIC(15,0)   ENCODE az64
	,DOC_LINE_ID_INT_1 NUMERIC(15,0)   ENCODE az64
	,DOC_LINE_ID_INT_2 NUMERIC(15,0)   ENCODE az64
	,DOC_LINE_ID_INT_3 NUMERIC(15,0)   ENCODE az64
	,DOC_LINE_ID_INT_4 NUMERIC(15,0)   ENCODE az64
	,DOC_LINE_ID_INT_5 NUMERIC(15,0)   ENCODE az64
	,DOC_LINE_ID_CHAR_1 VARCHAR(30)   ENCODE lzo
	,DOC_LINE_ID_CHAR_2 VARCHAR(30)   ENCODE lzo
	,DOC_LINE_ID_CHAR_3 VARCHAR(30)   ENCODE lzo
	,DOC_LINE_ID_CHAR_4 VARCHAR(30)   ENCODE lzo
	,DOC_LINE_ID_CHAR_5 VARCHAR(30)   ENCODE lzo
	,TAX_CALC_ACCTD_AMT NUMERIC(28,10)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64	
)
DISTSTYLE
auto;

INSERT INTO bec_ods.RA_CUSTOMER_TRX_LINES_ALL (
    customer_trx_line_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	customer_trx_id,
	line_number,
	set_of_books_id,
	reason_code,
	inventory_item_id,
	description,
	previous_customer_trx_id,
	previous_customer_trx_line_id,
	quantity_ordered,
	quantity_credited,
	quantity_invoiced,
	unit_standard_price,
	unit_selling_price,
	sales_order,
	sales_order_revision,
	sales_order_line,
	sales_order_date,
	accounting_rule_id,
	accounting_rule_duration,
	line_type,
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
	program_application_id,
	program_id,
	program_update_date,
	rule_start_date,
	initial_customer_trx_line_id,
	interface_line_context,
	interface_line_attribute1,
	interface_line_attribute2,
	interface_line_attribute3,
	interface_line_attribute4,
	interface_line_attribute5,
	interface_line_attribute6,
	interface_line_attribute7,
	interface_line_attribute8,
	sales_order_source,
	taxable_flag,
	extended_amount,
	revenue_amount,
	autorule_complete_flag,
	link_to_cust_trx_line_id,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	tax_precedence,
	tax_rate,
	item_exception_rate_id,
	tax_exemption_id,
	memo_line_id,
	autorule_duration_processed,
	uom_code,
	default_ussgl_transaction_code,
	default_ussgl_trx_code_context,
	interface_line_attribute10,
	interface_line_attribute11,
	interface_line_attribute12,
	interface_line_attribute13,
	interface_line_attribute14,
	interface_line_attribute15,
	interface_line_attribute9,
	vat_tax_id,
	autotax,
	last_period_to_credit,
	item_context,
	tax_exempt_flag,
	tax_exempt_number,
	tax_exempt_reason_code,
	tax_vendor_return_code,
	sales_tax_id,
	location_segment_id,
	movement_id,
	org_id,
	wh_update_date,
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
	gross_unit_selling_price,
	gross_extended_amount,
	amount_includes_tax_flag,
	taxable_amount,
	warehouse_id,
	translated_description,
	extended_acctd_amount,
	br_ref_customer_trx_id,
	br_ref_payment_schedule_id,
	br_adjustment_id,
	mrc_extended_acctd_amount,
	payment_set_id,
	contract_line_id,
	source_data_key1,
	source_data_key2,
	source_data_key3,
	source_data_key4,
	source_data_key5,
	invoiced_line_acctg_level,
	override_auto_accounting_flag,
	ship_to_customer_id,
	ship_to_address_id,
	ship_to_site_use_id,
	ship_to_contact_id,
	historical_flag,
	tax_line_id,
	line_recoverable,
	tax_recoverable,
	tax_classification_code,
	amount_due_remaining,
	acctd_amount_due_remaining,
	amount_due_original,
	acctd_amount_due_original,
	chrg_amount_remaining,
	chrg_acctd_amount_remaining,
	frt_adj_remaining,
	frt_adj_acctd_remaining,
	frt_ed_amount,
	frt_ed_acctd_amount,
	frt_uned_amount,
	frt_uned_acctd_amount,
	deferral_exclusion_flag,
	rule_end_date,
	payment_trxn_extension_id,
	interest_line_id,
	doc_line_id_int_1,
	doc_line_id_int_2,
	doc_line_id_int_3,
	doc_line_id_int_4,
	doc_line_id_int_5,
	doc_line_id_char_1,
	doc_line_id_char_2,
	doc_line_id_char_3,
	doc_line_id_char_4,
	doc_line_id_char_5,
	tax_calc_acctd_amt,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
    SELECT
        customer_trx_line_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	customer_trx_id,
	line_number,
	set_of_books_id,
	reason_code,
	inventory_item_id,
	description,
	previous_customer_trx_id,
	previous_customer_trx_line_id,
	quantity_ordered,
	quantity_credited,
	quantity_invoiced,
	unit_standard_price,
	unit_selling_price,
	sales_order,
	sales_order_revision,
	sales_order_line,
	sales_order_date,
	accounting_rule_id,
	accounting_rule_duration,
	line_type,
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
	program_application_id,
	program_id,
	program_update_date,
	rule_start_date,
	initial_customer_trx_line_id,
	interface_line_context,
	interface_line_attribute1,
	interface_line_attribute2,
	interface_line_attribute3,
	interface_line_attribute4,
	interface_line_attribute5,
	interface_line_attribute6,
	interface_line_attribute7,
	interface_line_attribute8,
	sales_order_source,
	taxable_flag,
	extended_amount,
	revenue_amount,
	autorule_complete_flag,
	link_to_cust_trx_line_id,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	tax_precedence,
	tax_rate,
	item_exception_rate_id,
	tax_exemption_id,
	memo_line_id,
	autorule_duration_processed,
	uom_code,
	default_ussgl_transaction_code,
	default_ussgl_trx_code_context,
	interface_line_attribute10,
	interface_line_attribute11,
	interface_line_attribute12,
	interface_line_attribute13,
	interface_line_attribute14,
	interface_line_attribute15,
	interface_line_attribute9,
	vat_tax_id,
	autotax,
	last_period_to_credit,
	item_context,
	tax_exempt_flag,
	tax_exempt_number,
	tax_exempt_reason_code,
	tax_vendor_return_code,
	sales_tax_id,
	location_segment_id,
	movement_id,
	org_id,
	wh_update_date,
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
	gross_unit_selling_price,
	gross_extended_amount,
	amount_includes_tax_flag,
	taxable_amount,
	warehouse_id,
	translated_description,
	extended_acctd_amount,
	br_ref_customer_trx_id,
	br_ref_payment_schedule_id,
	br_adjustment_id,
	mrc_extended_acctd_amount,
	payment_set_id,
	contract_line_id,
	source_data_key1,
	source_data_key2,
	source_data_key3,
	source_data_key4,
	source_data_key5,
	invoiced_line_acctg_level,
	override_auto_accounting_flag,
	ship_to_customer_id,
	ship_to_address_id,
	ship_to_site_use_id,
	ship_to_contact_id,
	historical_flag,
	tax_line_id,
	line_recoverable,
	tax_recoverable,
	tax_classification_code,
	amount_due_remaining,
	acctd_amount_due_remaining,
	amount_due_original,
	acctd_amount_due_original,
	chrg_amount_remaining,
	chrg_acctd_amount_remaining,
	frt_adj_remaining,
	frt_adj_acctd_remaining,
	frt_ed_amount,
	frt_ed_acctd_amount,
	frt_uned_amount,
	frt_uned_acctd_amount,
	deferral_exclusion_flag,
	rule_end_date,
	payment_trxn_extension_id,
	interest_line_id,
	doc_line_id_int_1,
	doc_line_id_int_2,
	doc_line_id_int_3,
	doc_line_id_int_4,
	doc_line_id_int_5,
	doc_line_id_char_1,
	doc_line_id_char_2,
	doc_line_id_char_3,
	doc_line_id_char_4,
	doc_line_id_char_5,
	tax_calc_acctd_amt,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.RA_CUSTOMER_TRX_LINES_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ra_customer_trx_lines_all';
	
COMMIT;