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

delete from bec_ods.RA_CUSTOMER_TRX_LINES_ALL
where CUSTOMER_TRX_LINE_ID in (
select stg.CUSTOMER_TRX_LINE_ID
from bec_ods.RA_CUSTOMER_TRX_LINES_ALL ods, bec_ods_stg.RA_CUSTOMER_TRX_LINES_ALL stg
where ods.CUSTOMER_TRX_LINE_ID = stg.CUSTOMER_TRX_LINE_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.RA_CUSTOMER_TRX_LINES_ALL
(customer_trx_line_id,
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
	,kca_seq_date)
(SELECT 
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
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.RA_CUSTOMER_TRX_LINES_ALL
	where kca_operation in ('INSERT','UPDATE') 
	and (CUSTOMER_TRX_LINE_ID,kca_seq_id) in 
	(select CUSTOMER_TRX_LINE_ID,max(kca_seq_id) from bec_ods_stg.RA_CUSTOMER_TRX_LINES_ALL 
     where kca_operation in ('INSERT','UPDATE')
     group by CUSTOMER_TRX_LINE_ID)
);

commit;



-- Soft delete
update bec_ods.RA_CUSTOMER_TRX_LINES_ALL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.RA_CUSTOMER_TRX_LINES_ALL set IS_DELETED_FLG = 'Y'
where (CUSTOMER_TRX_LINE_ID)  in
(
select CUSTOMER_TRX_LINE_ID from bec_raw_dl_ext.RA_CUSTOMER_TRX_LINES_ALL
where (CUSTOMER_TRX_LINE_ID,KCA_SEQ_ID)
in 
(
select CUSTOMER_TRX_LINE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.RA_CUSTOMER_TRX_LINES_ALL
group by CUSTOMER_TRX_LINE_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'ra_customer_trx_lines_all';

commit;