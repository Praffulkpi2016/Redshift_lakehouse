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

drop TABLE if exists bec_ods.ap_invoice_lines_all;

CREATE TABLE IF NOT EXISTS bec_ods.ap_invoice_lines_all
(
	invoice_id NUMERIC(15,0)   ENCODE az64
	,line_number NUMERIC(28,10)    ENCODE az64
	,line_type_lookup_code VARCHAR(25)   ENCODE lzo
	,requester_id NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
	,line_source VARCHAR(30)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,line_group_number NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,item_description VARCHAR(240)   ENCODE lzo
	,serial_number VARCHAR(35)   ENCODE lzo
	,manufacturer VARCHAR(30)   ENCODE lzo
	,model_number VARCHAR(40)   ENCODE lzo
	,warranty_number VARCHAR(80)   ENCODE lzo
	,generate_dists VARCHAR(1)   ENCODE lzo
	,match_type VARCHAR(25)   ENCODE lzo
	,distribution_set_id NUMERIC(15,0)   ENCODE az64
	,account_segment VARCHAR(25)   ENCODE lzo
	,balancing_segment VARCHAR(25)   ENCODE lzo
	,cost_center_segment VARCHAR(25)   ENCODE lzo
	,overlay_dist_code_concat VARCHAR(250)   ENCODE lzo
	,default_dist_ccid INTEGER   ENCODE az64
	,prorate_across_all_items VARCHAR(1)   ENCODE lzo
	,accounting_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,period_name VARCHAR(80)   ENCODE lzo
	,deferred_acctg_flag VARCHAR(1)   ENCODE lzo
	,def_acctg_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,def_acctg_end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,def_acctg_number_of_periods NUMERIC(15,0)   ENCODE az64
	,def_acctg_period_type VARCHAR(30)   ENCODE lzo
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,amount NUMERIC(28,10)   ENCODE az64
	,base_amount NUMERIC(28,10)   ENCODE az64
	,rounding_amt NUMERIC(28,10)   ENCODE az64
	,quantity_invoiced NUMERIC(28,10)   ENCODE az64
	,unit_meas_lookup_code VARCHAR(25)   ENCODE lzo
	,unit_price NUMERIC(28,10)   ENCODE az64
	,wfapproval_status VARCHAR(30)   ENCODE lzo
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,discarded_flag VARCHAR(1)   ENCODE lzo
	,original_amount NUMERIC(28,10)   ENCODE az64
	,original_base_amount NUMERIC(28,10)   ENCODE az64
	,original_rounding_amt NUMERIC(28,10)   ENCODE az64
	,cancelled_flag VARCHAR(1)   ENCODE lzo
	,income_tax_region VARCHAR(10)   ENCODE lzo
	,type_1099 VARCHAR(10)   ENCODE lzo
	,stat_amount NUMERIC(28,10)   ENCODE az64
	,prepay_invoice_id NUMERIC(15,0)   ENCODE az64
	,prepay_line_number NUMERIC(15,0)   ENCODE az64
	,invoice_includes_prepay_flag VARCHAR(1)   ENCODE lzo
	,corrected_inv_id NUMERIC(15,0)   ENCODE az64
	,corrected_line_number NUMERIC(15,0)   ENCODE az64
	,po_header_id NUMERIC(15,0)   ENCODE az64
	,po_line_id NUMERIC(15,0)   ENCODE az64
	,po_release_id NUMERIC(15,0)   ENCODE az64
	,po_line_location_id NUMERIC(15,0)   ENCODE az64
	,po_distribution_id NUMERIC(15,0)   ENCODE az64
	,rcv_transaction_id NUMERIC(15,0)   ENCODE az64
	,final_match_flag VARCHAR(1)   ENCODE lzo
	,assets_tracking_flag VARCHAR(1)   ENCODE lzo
	,asset_book_type_code VARCHAR(80)   ENCODE lzo
	,asset_category_id NUMERIC(15,0)   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,expenditure_type VARCHAR(30)   ENCODE lzo
	,expenditure_item_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,expenditure_organization_id NUMERIC(15,0)   ENCODE az64
	,pa_quantity NUMERIC(28,10)   ENCODE az64
	,pa_cc_ar_invoice_id NUMERIC(15,0)   ENCODE az64
	,pa_cc_ar_invoice_line_num NUMERIC(15,0)   ENCODE az64
	,pa_cc_processed_code VARCHAR(1)   ENCODE lzo
	,award_id NUMERIC(15,0)   ENCODE az64
	,awt_group_id NUMERIC(15,0)   ENCODE az64
	,reference_1 VARCHAR(30)   ENCODE lzo
	,reference_2 VARCHAR(30)   ENCODE lzo
	,receipt_verified_flag VARCHAR(1)   ENCODE lzo
	,receipt_required_flag VARCHAR(1)   ENCODE lzo
	,receipt_missing_flag VARCHAR(1)   ENCODE lzo
	,justification VARCHAR(240)   ENCODE lzo
	,expense_group VARCHAR(80)   ENCODE lzo
	,start_expense_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_expense_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,receipt_currency_code VARCHAR(80)   ENCODE lzo
	,receipt_conversion_rate NUMERIC(28,10)   ENCODE az64
	,receipt_currency_amount NUMERIC(28,10)   ENCODE az64
	,daily_amount NUMERIC(28,10)   ENCODE az64
	,web_parameter_id NUMERIC(15,0)   ENCODE az64
	,adjustment_reason VARCHAR(240)   ENCODE lzo
	,merchant_document_number VARCHAR(80)   ENCODE lzo
	,merchant_name VARCHAR(80)   ENCODE lzo
	,line_owner_role VARCHAR(320)   ENCODE lzo
	,disputable_flag VARCHAR(1)   ENCODE lzo
	,rcv_shipment_line_id NUMERIC(15,0)   ENCODE az64
	,pay_awt_group_id NUMERIC(15,0)   ENCODE az64
	,ail_invoice_id NUMERIC(15,0)   ENCODE az64
	,ail_distribution_line_number NUMERIC(15,0)   ENCODE az64
	,ail_invoice_id2 NUMERIC(15,0)   ENCODE az64
	,ail_distribution_line_number2 NUMERIC(15,0)   ENCODE az64
	,ail_invoice_id3 NUMERIC(15,0)   ENCODE az64
	,ail_distribution_line_number3 NUMERIC(15,0)   ENCODE az64
	,ail_invoice_id4 NUMERIC(15,0)   ENCODE az64
	,merchant_reference VARCHAR(240)   ENCODE lzo
	,merchant_tax_reg_number VARCHAR(80)   ENCODE lzo
	,merchant_taxpayer_id VARCHAR(80)   ENCODE lzo
	,country_of_supply VARCHAR(5)   ENCODE lzo
	,credit_card_trx_id NUMERIC(15,0)   ENCODE az64
	,company_prepaid_invoice_id NUMERIC(15,0)   ENCODE az64
	,cc_reversal_flag VARCHAR(1)   ENCODE lzo
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,attribute_category VARCHAR(150)   ENCODE lzo
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
	,line_selected_for_appl_flag VARCHAR(1)   ENCODE lzo
	,prepay_appl_request_id NUMERIC(15,0)   ENCODE az64
	,application_id NUMERIC(15,0)   ENCODE az64
	,product_table VARCHAR(30)   ENCODE lzo
	,reference_key1 VARCHAR(150)   ENCODE lzo
	,reference_key2 VARCHAR(150)   ENCODE lzo
	,reference_key3 VARCHAR(150)   ENCODE lzo
	,reference_key4 VARCHAR(150)   ENCODE lzo
	,reference_key5 VARCHAR(150)   ENCODE lzo
	,purchasing_category_id NUMERIC(15,0)   ENCODE az64
	,cost_factor_id NUMERIC(15,0)   ENCODE az64
	,control_amount NUMERIC(28,10)   ENCODE az64
	,assessable_value NUMERIC(28,10)   ENCODE az64
	,total_rec_tax_amount NUMERIC(28,10)   ENCODE az64
	,total_nrec_tax_amount NUMERIC(28,10)   ENCODE az64
	,total_rec_tax_amt_funcl_curr NUMERIC(28,10)   ENCODE az64
	,total_nrec_tax_amt_funcl_curr NUMERIC(28,10)   ENCODE az64
	,included_tax_amount NUMERIC(28,10)   ENCODE az64
	,primary_bigintended_use VARCHAR(30)   ENCODE lzo
	,tax_already_calculated_flag VARCHAR(1)   ENCODE lzo
	,ship_to_location_id NUMERIC(15,0)   ENCODE az64
	,product_type VARCHAR(240)   ENCODE lzo
	,product_category VARCHAR(240)   ENCODE lzo
	,product_fisc_classification VARCHAR(240)   ENCODE lzo
	,user_defined_fisc_class VARCHAR(240)   ENCODE lzo
	,trx_business_category VARCHAR(240)   ENCODE lzo
	,summary_tax_line_id NUMERIC(15,0)   ENCODE az64
	,tax_regime_code VARCHAR(30)   ENCODE lzo
	,tax VARCHAR(30)   ENCODE lzo
	,tax_jurisdiction_code VARCHAR(30)   ENCODE lzo
	,tax_status_code VARCHAR(30)   ENCODE lzo
	,tax_rate_id NUMERIC(15,0)   ENCODE az64
	,tax_rate_code VARCHAR(150)   ENCODE lzo
	,tax_rate NUMERIC(28,10)   ENCODE az64
	,tax_code_id NUMERIC(15,0)   ENCODE az64
	,historical_flag VARCHAR(1)   ENCODE lzo
	,tax_classification_code VARCHAR(30)   ENCODE lzo
	,source_application_id NUMERIC(15,0)   ENCODE az64
	,source_event_class_code VARCHAR(30)   ENCODE lzo
	,source_entity_code VARCHAR(30)   ENCODE lzo
	,source_trx_id NUMERIC(15,0)   ENCODE az64
	,source_line_id NUMERIC(15,0)   ENCODE az64
	,source_trx_level_type VARCHAR(30)   ENCODE lzo
	,retained_amount NUMERIC(28,10)   ENCODE az64
	,retained_amount_remaining NUMERIC(28,10)   ENCODE az64
	,retained_invoice_id NUMERIC(15,0)   ENCODE az64
	,retained_line_number NUMERIC(15,0)   ENCODE az64
	,line_selected_for_release_flag VARCHAR(1)   ENCODE lzo
	,"MERCHANT_NAME#1" VARCHAR(240)   ENCODE lzo	
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.ap_invoice_lines_all 
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
,kca_seq_id
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
,pay_awt_group_id,
ail_invoice_id,
ail_distribution_line_number,
ail_invoice_id2,
ail_distribution_line_number2,
ail_invoice_id3,
ail_distribution_line_number3,
ail_invoice_id4,
merchant_reference,
merchant_tax_reg_number,
merchant_taxpayer_id,
country_of_supply,
credit_card_trx_id,
company_prepaid_invoice_id,
cc_reversal_flag,
creation_date,
created_by,
last_updated_by,
last_update_date,
last_update_login,
program_application_id,
program_id,
program_update_date,
request_id,
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
line_selected_for_appl_flag,
prepay_appl_request_id,
application_id,
product_table,
reference_key1,
reference_key2,
reference_key3,
reference_key4,
reference_key5,
purchasing_category_id,
cost_factor_id,
control_amount,
assessable_value,
total_rec_tax_amount,
total_nrec_tax_amount,
total_rec_tax_amt_funcl_curr,
total_nrec_tax_amt_funcl_curr,
included_tax_amount,
tax_already_calculated_flag,
ship_to_location_id,
product_type,
product_category,
product_fisc_classification,
user_defined_fisc_class,
trx_business_category,
summary_tax_line_id,
tax_regime_code,
tax,
tax_jurisdiction_code,
tax_status_code,
tax_rate_id,
tax_rate_code,
tax_rate,
tax_code_id,
historical_flag,
tax_classification_code,
source_application_id,
source_event_class_code,
source_entity_code,
source_trx_id,
source_line_id,
source_trx_level_type,
retained_amount,
retained_amount_remaining,
retained_invoice_id,
retained_line_number,
line_selected_for_release_flag,
"MERCHANT_NAME#1",
KCA_OPERATION,
'N' as IS_DELETED_FLG,
cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
kca_seq_date
from bec_ods_stg.ap_invoice_lines_all);

end;

update bec_etl_ctrl.batch_ods_info set load_type='I', last_refresh_date = getdate() where ods_table_name='ap_invoice_lines_all';

commit;