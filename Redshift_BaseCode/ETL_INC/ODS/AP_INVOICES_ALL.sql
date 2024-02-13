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

delete from bec_ods.ap_invoices_all
where INVOICE_ID in (
select stg.INVOICE_ID 
from bec_ods.ap_invoices_all ods, bec_ods_stg.ap_invoices_all stg
where ods.INVOICE_ID = stg.INVOICE_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into bec_ods.ap_invoices_all
(pay_curr_invoice_amount
,mrc_base_amount
,mrc_exchange_rate
,mrc_exchange_rate_type
,mrc_exchange_date
,gl_date
,award_id
,paid_on_behalf_employee_id
,amt_due_ccard_company
,amt_due_employee
,approval_ready_flag
,approval_iteration
,wfapproval_status
,requester_id
,validation_request_id
,validated_tax_amount
,quick_credit
,credited_invoice_id
,distribution_set_id
,application_id
,product_table
,reference_key1
,reference_key2
,reference_key3
,reference_key4
,reference_key5
,total_tax_amount
,self_assessed_tax_amount
,tax_related_invoice_id
,trx_business_category
,user_defined_fisc_class
,taxation_country
,document_sub_type
,supplier_tax_invoice_number
,supplier_tax_invoice_date
,supplier_tax_exchange_rate
,tax_invoice_recording_date
,tax_invoice_internal_seq
,legal_entity_id
,historical_flag
,force_revalidation_flag
,bank_charge_bearer
,remittance_message1
,remittance_message2
,remittance_message3
,unique_remittance_identifier
,uri_check_digit
,settlement_priority
,payment_reason_code
,payment_reason_comments
,payment_method_code
,delivery_channel_code
,quick_po_header_id
,net_of_retainage_flag
,release_amount_net_of_tax
,control_amount
,party_id
,party_site_id
,pay_proc_trxn_type_code
,payment_function
,cust_registration_code
,cust_registration_number
,port_of_entry_code
,external_bank_account_id
,vendor_contact_id
,internal_contact_email
,disc_is_inv_less_tax_flag
,exclude_freight_from_discount
,pay_awt_group_id
,original_invoice_amount
,dispute_reason
,remit_to_supplier_name
,remit_to_supplier_id
,remit_to_supplier_site
,remit_to_supplier_site_id
,relationship_id
,payment_cross_rate
,invoice_amount
,vendor_site_id
,amount_paid
,discount_amount_taken
,invoice_date
,source
,invoice_type_lookup_code
,description
,batch_id
,amount_applicable_to_discount
,tax_amount
,terms_id
,terms_date
,payment_method_lookup_code
,pay_group_lookup_code
,accts_pay_code_combination_id
,payment_status_flag
,creation_date
,created_by
,base_amount
,vat_code
,last_update_login
,exclusive_payment_flag
,po_header_id
,freight_amount
,goods_received_date
,invoice_received_date
,voucher_num
,approved_amount
,recurring_payment_id
,exchange_rate
,exchange_rate_type
,exchange_date
,earliest_settlement_date
,original_prepayment_amount
,doc_sequence_id
,doc_sequence_value
,doc_category_code
,attribute1
,attribute2
,attribute3
,attribute4
,attribute5
,attribute6
,attribute7
,attribute8
,attribute9
,attribute10
,attribute11
,attribute12
,attribute13
,attribute14
,attribute15
,attribute_category
,approval_status
,approval_description
,invoice_distribution_total
,posting_status
,prepay_flag
,authorized_by
,cancelled_date
,cancelled_by
,cancelled_amount
,temp_cancelled_amount
,project_accounting_context
,ussgl_transaction_code
,ussgl_trx_code_context
,project_id
,task_id
,expenditure_type
,expenditure_item_date
,pa_quantity
,expenditure_organization_id
,pa_default_dist_ccid
,vendor_prepay_amount
,payment_amount_total
,awt_flag
,awt_group_id
,reference_1
,reference_2
,org_id
,pre_withholding_amount
,global_attribute_category
,global_attribute1
,global_attribute2
,global_attribute3
,global_attribute4
,global_attribute5
,global_attribute6
,global_attribute7
,global_attribute8
,global_attribute9
,global_attribute10
,global_attribute11
,global_attribute12
,global_attribute13
,global_attribute14
,global_attribute15
,global_attribute16
,global_attribute17
,global_attribute18
,global_attribute19
,global_attribute20
,auto_tax_calc_flag
,payment_cross_rate_type
,payment_cross_rate_date
,invoice_id
,last_update_date
,last_updated_by
,vendor_id
,invoice_num
,set_of_books_id
,invoice_currency_code
,payment_currency_code
,po_matched_flag
,validation_worker_id
,"pay_group_lookup_code#1"
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date)
(select
pay_curr_invoice_amount
,mrc_base_amount
,mrc_exchange_rate
,mrc_exchange_rate_type
,mrc_exchange_date
,gl_date
,award_id
,paid_on_behalf_employee_id
,amt_due_ccard_company
,amt_due_employee
,approval_ready_flag
,approval_iteration
,wfapproval_status
,requester_id
,validation_request_id
,validated_tax_amount
,quick_credit
,credited_invoice_id
,distribution_set_id
,application_id
,product_table
,reference_key1
,reference_key2
,reference_key3
,reference_key4
,reference_key5
,total_tax_amount
,self_assessed_tax_amount
,tax_related_invoice_id
,trx_business_category
,user_defined_fisc_class
,taxation_country
,document_sub_type
,supplier_tax_invoice_number
,supplier_tax_invoice_date
,supplier_tax_exchange_rate
,tax_invoice_recording_date
,tax_invoice_internal_seq
,legal_entity_id
,historical_flag
,force_revalidation_flag
,bank_charge_bearer
,remittance_message1
,remittance_message2
,remittance_message3
,unique_remittance_identifier
,uri_check_digit
,settlement_priority
,payment_reason_code
,payment_reason_comments
,payment_method_code
,delivery_channel_code
,quick_po_header_id
,net_of_retainage_flag
,release_amount_net_of_tax
,control_amount
,party_id
,party_site_id
,pay_proc_trxn_type_code
,payment_function
,cust_registration_code
,cust_registration_number
,port_of_entry_code
,external_bank_account_id
,vendor_contact_id
,internal_contact_email
,disc_is_inv_less_tax_flag
,exclude_freight_from_discount
,pay_awt_group_id
,original_invoice_amount
,dispute_reason
,remit_to_supplier_name
,remit_to_supplier_id
,remit_to_supplier_site
,remit_to_supplier_site_id
,relationship_id
,payment_cross_rate
,invoice_amount
,vendor_site_id
,amount_paid
,discount_amount_taken
,invoice_date
,source
,invoice_type_lookup_code
,description
,batch_id
,amount_applicable_to_discount
,tax_amount
,terms_id
,terms_date
,payment_method_lookup_code
,pay_group_lookup_code
,accts_pay_code_combination_id
,payment_status_flag
,creation_date
,created_by
,base_amount
,vat_code
,last_update_login
,exclusive_payment_flag
,po_header_id
,freight_amount
,goods_received_date
,invoice_received_date
,voucher_num
,approved_amount
,recurring_payment_id
,exchange_rate
,exchange_rate_type
,exchange_date
,earliest_settlement_date
,original_prepayment_amount
,doc_sequence_id
,doc_sequence_value
,doc_category_code
,attribute1
,attribute2
,attribute3
,attribute4
,attribute5
,attribute6
,attribute7
,attribute8
,attribute9
,attribute10
,attribute11
,attribute12
,attribute13
,attribute14
,attribute15
,attribute_category
,approval_status
,approval_description
,invoice_distribution_total
,posting_status
,prepay_flag
,authorized_by
,cancelled_date
,cancelled_by
,cancelled_amount
,temp_cancelled_amount
,project_accounting_context
,ussgl_transaction_code
,ussgl_trx_code_context
,project_id
,task_id
,expenditure_type
,expenditure_item_date
,pa_quantity
,expenditure_organization_id
,pa_default_dist_ccid
,vendor_prepay_amount
,payment_amount_total
,awt_flag
,awt_group_id
,reference_1
,reference_2
,org_id
,pre_withholding_amount
,global_attribute_category
,global_attribute1
,global_attribute2
,global_attribute3
,global_attribute4
,global_attribute5
,global_attribute6
,global_attribute7
,global_attribute8
,global_attribute9
,global_attribute10
,global_attribute11
,global_attribute12
,global_attribute13
,global_attribute14
,global_attribute15
,global_attribute16
,global_attribute17
,global_attribute18
,global_attribute19
,global_attribute20
,auto_tax_calc_flag
,payment_cross_rate_type
,payment_cross_rate_date
,invoice_id
,last_update_date
,last_updated_by
,vendor_id
,invoice_num
,set_of_books_id
,invoice_currency_code
,payment_currency_code
,po_matched_flag
,validation_worker_id
,"pay_group_lookup_code#1"
,KCA_OPERATION
,'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from bec_ods_stg.ap_invoices_all
where kca_operation IN ('INSERT','UPDATE') 
	and (INVOICE_ID,kca_seq_id) in 
	(select INVOICE_ID,max(kca_seq_id) from bec_ods_stg.ap_invoices_all 
     where kca_operation IN ('INSERT','UPDATE')
     group by INVOICE_ID)
);

commit;

-- Soft delete
update bec_ods.ap_invoices_all set IS_DELETED_FLG = 'N';
commit;
update bec_ods.ap_invoices_all set IS_DELETED_FLG = 'Y'
where (INVOICE_ID)  in
(
select INVOICE_ID from bec_raw_dl_ext.ap_invoices_all
where (INVOICE_ID,KCA_SEQ_ID)
in 
(
select INVOICE_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ap_invoices_all
group by INVOICE_ID
) 
and kca_operation= 'DELETE'
);
commit;


end;

update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='ap_invoices_all';
commit;