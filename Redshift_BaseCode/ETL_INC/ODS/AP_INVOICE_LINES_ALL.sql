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
BEGIN;

-- Delete Records

delete from bec_ods.AP_INVOICE_LINES_ALL
where (invoice_id,line_number) in (
select stg.invoice_id,stg.line_number
from bec_ods.AP_INVOICE_LINES_ALL ods, bec_ods_stg.AP_INVOICE_LINES_ALL stg
where ods.invoice_id = stg.invoice_id
and trunc(ods.line_number) = trunc(stg.line_number)
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into bec_ods.AP_INVOICE_LINES_ALL
(invoice_id
,line_number
,line_type_lookup_code
,requester_id
,description
,line_source
,org_id
,line_group_number
,inventory_item_id
,item_description
,serial_number
,manufacturer
,model_number
,warranty_number
,generate_dists
,match_type
,distribution_set_id
,account_segment
,balancing_segment
,cost_center_segment
,overlay_dist_code_concat
,default_dist_ccid
,prorate_across_all_items
,accounting_date
,period_name
,deferred_acctg_flag
,def_acctg_start_date
,def_acctg_end_date
,def_acctg_number_of_periods
,def_acctg_period_type
,set_of_books_id
,amount
,base_amount
,rounding_amt
,quantity_invoiced
,unit_meas_lookup_code
,unit_price
,wfapproval_status
,ussgl_transaction_code
,discarded_flag
,original_amount
,original_base_amount
,original_rounding_amt
,cancelled_flag
,income_tax_region
,type_1099
,stat_amount
,prepay_invoice_id
,prepay_line_number
,invoice_includes_prepay_flag
,corrected_inv_id
,corrected_line_number
,po_header_id
,po_line_id
,po_release_id
,po_line_location_id
,po_distribution_id
,rcv_transaction_id
,final_match_flag
,assets_tracking_flag
,asset_book_type_code
,asset_category_id
,project_id
,task_id
,expenditure_type
,expenditure_item_date
,expenditure_organization_id
,pa_quantity
,pa_cc_ar_invoice_id
,pa_cc_ar_invoice_line_num
,pa_cc_processed_code
,award_id
,awt_group_id
,reference_1
,reference_2
,receipt_verified_flag
,receipt_required_flag
,receipt_missing_flag
,justification
,expense_group
,start_expense_date
,end_expense_date
,receipt_currency_code
,receipt_conversion_rate
,receipt_currency_amount
,daily_amount
,web_parameter_id
,adjustment_reason
,merchant_document_number
,merchant_name
,line_owner_role
,disputable_flag
,rcv_shipment_line_id
,pay_awt_group_id
,ail_invoice_id
,ail_distribution_line_number
,ail_invoice_id2
,ail_distribution_line_number2
,ail_invoice_id3
,ail_distribution_line_number3
,ail_invoice_id4
,merchant_reference
,merchant_tax_reg_number
,merchant_taxpayer_id
,country_of_supply
,credit_card_trx_id
,company_prepaid_invoice_id
,cc_reversal_flag
,creation_date
,created_by
,last_updated_by
,last_update_date
,last_update_login
,program_application_id
,program_id
,program_update_date
,request_id
,attribute_category
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
,line_selected_for_appl_flag
,prepay_appl_request_id
,application_id
,product_table
,reference_key1
,reference_key2
,reference_key3
,reference_key4
,reference_key5
,purchasing_category_id
,cost_factor_id
,control_amount
,assessable_value
,total_rec_tax_amount
,total_nrec_tax_amount
,total_rec_tax_amt_funcl_curr
,total_nrec_tax_amt_funcl_curr
,included_tax_amount
--,primary_bigintended_use
,tax_already_calculated_flag
,ship_to_location_id
,product_type
,product_category
,product_fisc_classification
,user_defined_fisc_class
,trx_business_category
,summary_tax_line_id
,tax_regime_code
,tax
,tax_jurisdiction_code
,tax_status_code
,tax_rate_id
,tax_rate_code
,tax_rate
,tax_code_id
,historical_flag
,tax_classification_code
,source_application_id
,source_event_class_code
,source_entity_code
,source_trx_id
,source_line_id
,source_trx_level_type
,retained_amount
,retained_amount_remaining
,retained_invoice_id
,retained_line_number
,line_selected_for_release_flag
,"MERCHANT_NAME#1"
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date) 
(select
invoice_id
,trunc(line_number) as line_number
,line_type_lookup_code
,requester_id
,description
,line_source
,org_id
,line_group_number
,inventory_item_id
,item_description
,serial_number
,manufacturer
,model_number
,warranty_number
,generate_dists
,match_type
,distribution_set_id
,account_segment
,balancing_segment
,cost_center_segment
,overlay_dist_code_concat
,default_dist_ccid
,prorate_across_all_items
,accounting_date
,period_name
,deferred_acctg_flag
,def_acctg_start_date
,def_acctg_end_date
,def_acctg_number_of_periods
,def_acctg_period_type
,set_of_books_id
,amount
,base_amount
,rounding_amt
,quantity_invoiced
,unit_meas_lookup_code
,unit_price
,wfapproval_status
,ussgl_transaction_code
,discarded_flag
,original_amount
,original_base_amount
,original_rounding_amt
,cancelled_flag
,income_tax_region
,type_1099
,stat_amount
,prepay_invoice_id
,prepay_line_number
,invoice_includes_prepay_flag
,corrected_inv_id
,corrected_line_number
,po_header_id
,po_line_id
,po_release_id
,po_line_location_id
,po_distribution_id
,rcv_transaction_id
,final_match_flag
,assets_tracking_flag
,asset_book_type_code
,asset_category_id
,project_id
,task_id
,expenditure_type
,expenditure_item_date
,expenditure_organization_id
,pa_quantity
,pa_cc_ar_invoice_id
,pa_cc_ar_invoice_line_num
,pa_cc_processed_code
,award_id
,awt_group_id
,reference_1
,reference_2
,receipt_verified_flag
,receipt_required_flag
,receipt_missing_flag
,justification
,expense_group
,start_expense_date
,end_expense_date
,receipt_currency_code
,receipt_conversion_rate
,receipt_currency_amount
,daily_amount
,web_parameter_id
,adjustment_reason
,merchant_document_number
,merchant_name
,line_owner_role
,disputable_flag
,rcv_shipment_line_id
,pay_awt_group_id
,ail_invoice_id
,ail_distribution_line_number
,ail_invoice_id2
,ail_distribution_line_number2
,ail_invoice_id3
,ail_distribution_line_number3
,ail_invoice_id4
,merchant_reference
,merchant_tax_reg_number
,merchant_taxpayer_id
,country_of_supply
,credit_card_trx_id
,company_prepaid_invoice_id
,cc_reversal_flag
,creation_date
,created_by
,last_updated_by
,last_update_date
,last_update_login
,program_application_id
,program_id
,program_update_date
,request_id
,attribute_category
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
,line_selected_for_appl_flag
,prepay_appl_request_id
,application_id
,product_table
,reference_key1
,reference_key2
,reference_key3
,reference_key4
,reference_key5
,purchasing_category_id
,cost_factor_id
,control_amount
,assessable_value
,total_rec_tax_amount
,total_nrec_tax_amount
,total_rec_tax_amt_funcl_curr
,total_nrec_tax_amt_funcl_curr
,included_tax_amount
--,primary_bigintended_use
,tax_already_calculated_flag
,ship_to_location_id
,product_type
,product_category
,product_fisc_classification
,user_defined_fisc_class
,trx_business_category
,summary_tax_line_id
,tax_regime_code
,tax
,tax_jurisdiction_code
,tax_status_code
,tax_rate_id
,tax_rate_code
,tax_rate
,tax_code_id
,historical_flag
,tax_classification_code
,source_application_id
,source_event_class_code
,source_entity_code
,source_trx_id
,source_line_id
,source_trx_level_type
,retained_amount
,retained_amount_remaining
,retained_invoice_id
,retained_line_number
,line_selected_for_release_flag
,"MERCHANT_NAME#1"
,KCA_OPERATION
,'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date                   
from bec_ods_stg.AP_INVOICE_LINES_ALL
where kca_operation IN ('INSERT','UPDATE') 
	and (invoice_id,line_number,kca_seq_id) in 
	(select invoice_id,line_number,max(kca_seq_id) from bec_ods_stg.AP_INVOICE_LINES_ALL 
     where kca_operation IN ('INSERT','UPDATE')
     group by invoice_id,line_number)
);

commit;

-- Soft delete
update bec_ods.AP_INVOICE_LINES_ALL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.AP_INVOICE_LINES_ALL set IS_DELETED_FLG = 'Y'
where (invoice_id,line_number)  in
(
select invoice_id,line_number from bec_raw_dl_ext.AP_INVOICE_LINES_ALL
where (invoice_id,line_number,KCA_SEQ_ID)
in 
(
select invoice_id,line_number,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.AP_INVOICE_LINES_ALL
group by invoice_id,line_number
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='ap_invoice_lines_all';
commit;