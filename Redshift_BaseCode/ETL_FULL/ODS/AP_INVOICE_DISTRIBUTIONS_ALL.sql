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

drop TABLE if exists bec_ods.ap_invoice_distributions_all;

CREATE TABLE IF NOT EXISTS bec_ods.ap_invoice_distributions_all
(
	accounting_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,accounting_event_id NUMERIC(15,0)   ENCODE az64
	,accrual_posted_flag VARCHAR(1)   ENCODE lzo
	,accts_pay_code_combination_id NUMERIC(15,0)   ENCODE az64
	,adjustment_reason VARCHAR(240)   ENCODE lzo
	,amount NUMERIC(28,10)   ENCODE az64
	,amount_at_prepay_pay_xrate NUMERIC(28,10)   ENCODE az64
	,amount_at_prepay_xrate NUMERIC(28,10)   ENCODE az64
	,amount_encumbered NUMERIC(28,10)   ENCODE az64
	,amount_includes_tax_flag VARCHAR(1)   ENCODE lzo
	,amount_to_post NUMERIC(28,10)   ENCODE az64
	,amount_variance NUMERIC(28,10)   ENCODE az64
	,asset_book_type_code VARCHAR(15)   ENCODE lzo
	,asset_category_id NUMERIC(15,0)   ENCODE az64
	,assets_addition_flag VARCHAR(1)   ENCODE lzo
	,assets_tracking_flag VARCHAR(1)   ENCODE lzo
	,attribute_category VARCHAR(150)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,award_id NUMERIC(15,0)   ENCODE az64
	,awt_flag VARCHAR(1)   ENCODE lzo
	,awt_gross_amount NUMERIC(28,10)   ENCODE az64
	,awt_group_id NUMERIC(15,0)   ENCODE az64
	,awt_invoice_id NUMERIC(15,0)   ENCODE az64
	,awt_invoice_payment_id NUMERIC(15,0)   ENCODE az64
	,awt_origin_group_id NUMERIC(15,0)   ENCODE az64
	,awt_related_id NUMERIC(15,0)   ENCODE az64
	,awt_tax_rate_id NUMERIC(15,0)   ENCODE az64
	,awt_withheld_amt NUMERIC(28,10)   ENCODE az64
	,base_amount NUMERIC(28,10)   ENCODE az64
	,base_amount_encumbered NUMERIC(28,10)   ENCODE az64
	,base_amount_to_post NUMERIC(28,10)   ENCODE az64
	,base_amount_variance NUMERIC(28,10)   ENCODE az64
	,base_invoice_price_variance NUMERIC(28,10)   ENCODE az64
	,base_quantity_variance NUMERIC(28,10)   ENCODE az64
	,batch_id NUMERIC(15,0)   ENCODE az64
	,bc_event_id NUMERIC(15,0)   ENCODE az64
	,cancellation_flag VARCHAR(1)   ENCODE lzo
	,cancelled_flag VARCHAR(1)   ENCODE lzo
	,cash_basis_final_app_rounding NUMERIC(28,10)   ENCODE az64
	,cash_je_batch_id NUMERIC(15,0)   ENCODE az64
	,cash_posted_flag VARCHAR(1)   ENCODE lzo
	,cc_reversal_flag VARCHAR(1)   ENCODE lzo
	,charge_applicable_to_dist_id NUMERIC(15,0)   ENCODE az64
	,company_prepaid_invoice_id NUMERIC(15,0)   ENCODE az64
	,corrected_invoice_dist_id NUMERIC(15,0)   ENCODE az64
	,corrected_quantity NUMERIC(28,10)   ENCODE az64
	,country_of_supply VARCHAR(5)   ENCODE lzo
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,credit_card_trx_id NUMERIC(15,0)   ENCODE az64
	,daily_amount NUMERIC(28,10)   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
	,detail_tax_dist_id NUMERIC(15,0)   ENCODE az64
	,dist_code_combination_id NUMERIC(15,0)   ENCODE az64
	,dist_match_type VARCHAR(25)   ENCODE lzo
	,distribution_class VARCHAR(30)   ENCODE lzo
	,distribution_line_number NUMERIC(15,0)   ENCODE az64
	,earliest_settlement_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,encumbered_flag VARCHAR(1)   ENCODE lzo
	,end_expense_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,exchange_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,exchange_rate NUMERIC(28,10)   ENCODE az64
	,exchange_rate_type VARCHAR(30)   ENCODE lzo
	,exchange_rate_variance NUMERIC(28,10)   ENCODE az64
	,expenditure_item_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,expenditure_organization_id NUMERIC(15,0)   ENCODE az64
	,expenditure_type VARCHAR(30)   ENCODE lzo
	,expense_group VARCHAR(80)   ENCODE lzo
	,extra_po_erv NUMERIC(28,10)   ENCODE az64
	,final_application_rounding NUMERIC(28,10)   ENCODE az64
	,final_match_flag VARCHAR(1)   ENCODE lzo
	,final_payment_rounding NUMERIC(28,10)   ENCODE az64
	,final_release_rounding NUMERIC(28,10)   ENCODE az64
	,fully_paid_acctd_flag VARCHAR(1)   ENCODE lzo
	,global_attribute_category VARCHAR(150)   ENCODE lzo
	,global_attribute1 VARCHAR(150)   ENCODE lzo
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
	,global_attribute2 VARCHAR(150)   ENCODE lzo
	,global_attribute20 VARCHAR(150)   ENCODE lzo
	,global_attribute3 VARCHAR(150)   ENCODE lzo
	,global_attribute4 VARCHAR(150)   ENCODE lzo
	,global_attribute5 VARCHAR(150)   ENCODE lzo
	,global_attribute6 VARCHAR(150)   ENCODE lzo
	,global_attribute7 VARCHAR(150)   ENCODE lzo
	,global_attribute8 VARCHAR(150)   ENCODE lzo
	,global_attribute9 VARCHAR(150)   ENCODE lzo
	,gms_burdenable_raw_cost NUMERIC(22,5)   ENCODE az64
	,historical_flag VARCHAR(1)   ENCODE lzo
	,income_tax_region VARCHAR(10)   ENCODE lzo
	,intended_use VARCHAR(30)   ENCODE lzo
	,inventory_transfer_status VARCHAR(1)   ENCODE lzo
	,invoice_distribution_id NUMERIC(15,0)   ENCODE az64
	,invoice_id NUMERIC(15,0)   ENCODE az64
	,invoice_includes_prepay_flag VARCHAR(1)   ENCODE lzo
	,invoice_line_number NUMERIC(28,10)   ENCODE az64
	,invoice_price_variance NUMERIC(28,10)   ENCODE az64
	,je_batch_id NUMERIC(15,0)   ENCODE az64
	,justification VARCHAR(240)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,line_group_number NUMERIC(15,0)   ENCODE az64
	,line_type_lookup_code VARCHAR(25)   ENCODE lzo
	,match_status_flag VARCHAR(1)   ENCODE lzo
	,matched_uom_lookup_code VARCHAR(25)   ENCODE lzo
	,merchant_document_number VARCHAR(80)   ENCODE lzo
	,merchant_name VARCHAR(80)   ENCODE lzo
	,merchant_reference VARCHAR(240)   ENCODE lzo
	,merchant_tax_reg_number VARCHAR(80)   ENCODE lzo
	,merchant_taxpayer_id VARCHAR(80)   ENCODE lzo
	,mrc_accrual_posted_flag VARCHAR(2000)   ENCODE lzo
	,mrc_amount VARCHAR(2000)   ENCODE lzo
	,mrc_amount_to_post VARCHAR(2000)   ENCODE lzo
	,mrc_base_amount VARCHAR(2000)   ENCODE lzo
	,mrc_base_amount_to_post VARCHAR(2000)   ENCODE lzo
	,mrc_base_inv_price_variance VARCHAR(2000)   ENCODE lzo
	,mrc_cash_je_batch_id VARCHAR(2000)   ENCODE lzo
	,mrc_cash_posted_flag VARCHAR(2000)   ENCODE lzo
	,mrc_dist_code_combination_id VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_date VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_rate VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_rate_type VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_rate_variance VARCHAR(2000)   ENCODE lzo
	,mrc_je_batch_id VARCHAR(2000)   ENCODE lzo
	,mrc_posted_amount VARCHAR(2000)   ENCODE lzo
	,mrc_posted_base_amount VARCHAR(2000)   ENCODE lzo
	,mrc_posted_flag VARCHAR(2000)   ENCODE lzo
	,mrc_program_application_id VARCHAR(2000)   ENCODE lzo
	,mrc_program_id VARCHAR(2000)   ENCODE lzo
	,mrc_program_update_date VARCHAR(2000)   ENCODE lzo
	,mrc_rate_var_ccid VARCHAR(2000)   ENCODE lzo
	,mrc_receipt_conversion_rate VARCHAR(2000)   ENCODE lzo
	,mrc_request_id VARCHAR(2000)   ENCODE lzo
	,old_dist_line_number NUMERIC(15,0)   ENCODE az64
	,old_distribution_id NUMERIC(15,0)   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,other_invoice_id NUMERIC(15,0)   ENCODE az64
	,pa_addition_flag VARCHAR(1)   ENCODE lzo
	,pa_cc_ar_invoice_id NUMERIC(15,0)   ENCODE az64
	,pa_cc_ar_invoice_line_num NUMERIC(15,0)   ENCODE az64
	,pa_cc_processed_code VARCHAR(1)   ENCODE lzo
	,pa_cmt_xface_flag VARCHAR(1)   ENCODE lzo
	,pa_quantity NUMERIC(22,5)   ENCODE az64
	,packet_id NUMERIC(15,0)   ENCODE az64
	,parent_invoice_id NUMERIC(15,0)   ENCODE az64
	,parent_reversal_id NUMERIC(15,0)   ENCODE az64
	,pay_awt_group_id NUMERIC(15,0)   ENCODE az64
	,period_name VARCHAR(15)   ENCODE lzo
	,po_distribution_id NUMERIC(15,0)   ENCODE az64
	,posted_amount NUMERIC(28,10)   ENCODE az64
	,posted_base_amount NUMERIC(28,10)   ENCODE az64
	,posted_flag VARCHAR(1)   ENCODE lzo
	,prepay_amount_remaining NUMERIC(28,10)   ENCODE az64
	,prepay_distribution_id NUMERIC(15,0)   ENCODE az64
	,prepay_tax_diff_amount NUMERIC(28,10)   ENCODE az64
	,prepay_tax_parent_id NUMERIC(15,0)   ENCODE az64
	,price_adjustment_flag VARCHAR(1)   ENCODE lzo
	,price_correct_inv_id NUMERIC(15,0)   ENCODE az64
	,price_correct_qty NUMERIC(28,10)   ENCODE az64
	,price_var_code_combination_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,project_accounting_context VARCHAR(30)   ENCODE lzo
	,project_id NUMERIC(15,0)   ENCODE az64
	,quantity_invoiced NUMERIC(28,10)   ENCODE az64
	,quantity_unencumbered NUMERIC(28,10)   ENCODE az64
	,quantity_variance NUMERIC(28,10)   ENCODE az64
	,rate_var_code_combination_id NUMERIC(15,0)   ENCODE az64
	,rcv_charge_addition_flag VARCHAR(1)   ENCODE lzo
	,rcv_transaction_id NUMERIC(15,0)   ENCODE az64
	,rec_nrec_rate NUMERIC(28,10)   ENCODE az64
	,receipt_conversion_rate NUMERIC(28,10)   ENCODE az64
	,receipt_currency_amount NUMERIC(28,10)   ENCODE az64
	,receipt_currency_code VARCHAR(15)   ENCODE lzo
	,receipt_missing_flag VARCHAR(1)   ENCODE lzo
	,receipt_required_flag VARCHAR(1)   ENCODE lzo
	,receipt_verified_flag VARCHAR(1)   ENCODE lzo
	,recovery_rate_code VARCHAR(30)   ENCODE lzo
	,recovery_rate_id NUMERIC(15,0)   ENCODE az64
	,recovery_rate_name VARCHAR(150)   ENCODE lzo
	,recovery_type_code VARCHAR(30)   ENCODE lzo
	,recurring_payment_id NUMERIC(15,0)   ENCODE az64
	,reference_1 VARCHAR(30)   ENCODE lzo
	,reference_2 VARCHAR(30)   ENCODE lzo
	,related_id NUMERIC(15,0)   ENCODE az64
	,related_retainage_dist_id NUMERIC(15,0)   ENCODE az64
	,release_inv_dist_derived_from NUMERIC(15,0)   ENCODE az64
	,req_distribution_id NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,retained_amount_remaining NUMERIC(28,10)   ENCODE az64
	,retained_invoice_dist_id NUMERIC(15,0)   ENCODE az64
	,reversal_flag VARCHAR(1)   ENCODE lzo
	,root_distribution_id NUMERIC(15,0)   ENCODE az64
	,rounding_amt NUMERIC(28,10)   ENCODE az64
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,start_expense_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,stat_amount NUMERIC(28,10)   ENCODE az64
	,summary_tax_line_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,tax_already_distributed_flag VARCHAR(1)   ENCODE lzo
	,tax_calculated_flag VARCHAR(1)   ENCODE lzo
	,tax_code_id NUMERIC(15,0)   ENCODE az64
	,tax_code_override_flag VARCHAR(1)   ENCODE lzo
	,tax_recoverable_flag VARCHAR(1)   ENCODE lzo
	,tax_recovery_override_flag VARCHAR(1)   ENCODE lzo
	,tax_recovery_rate NUMERIC(28,10)   ENCODE az64
	,taxable_amount NUMERIC(28,10)   ENCODE az64
	,taxable_base_amount NUMERIC(28,10)   ENCODE az64
	,total_dist_amount NUMERIC(28,10)   ENCODE az64
	,total_dist_base_amount NUMERIC(28,10)   ENCODE az64
	,type_1099 VARCHAR(10)   ENCODE lzo
	,unit_price NUMERIC(28,10)   ENCODE az64
	,upgrade_base_posted_amt NUMERIC(28,10)   ENCODE az64
	,upgrade_posted_amt NUMERIC(28,10)   ENCODE az64
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,ussgl_trx_code_context VARCHAR(30)   ENCODE lzo
	,vat_code VARCHAR(15)   ENCODE lzo
	,web_parameter_id NUMERIC(15,0)   ENCODE az64
	,withholding_tax_code_id NUMERIC(15,0)   ENCODE az64
	,xinv_parent_reversal_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;
insert into bec_ods.ap_invoice_distributions_all
(accounting_date
,accounting_event_id
,accrual_posted_flag
,accts_pay_code_combination_id
,adjustment_reason
,amount
,amount_at_prepay_pay_xrate
,amount_at_prepay_xrate
,amount_encumbered
,amount_includes_tax_flag
,amount_to_post
,amount_variance
,asset_book_type_code
,asset_category_id
,assets_addition_flag
,assets_tracking_flag
,attribute_category
,attribute1
,attribute10
,attribute11
,attribute12
,attribute13
,attribute14
,attribute15
,attribute2
,attribute3
,attribute4
,attribute5
,attribute6
,attribute7
,attribute8
,attribute9
,award_id
,awt_flag
,awt_gross_amount
,awt_group_id
,awt_invoice_id
,awt_invoice_payment_id
,awt_origin_group_id
,awt_related_id
,awt_tax_rate_id
,AWT_WITHHELD_AMT
,base_amount
,base_amount_encumbered
,base_amount_to_post
,base_amount_variance
,base_invoice_price_variance
,base_quantity_variance
,batch_id
,bc_event_id
,cancellation_flag
,cancelled_flag
,cash_basis_final_app_rounding
,cash_je_batch_id
,cash_posted_flag
,cc_reversal_flag
,charge_applicable_to_dist_id
,company_prepaid_invoice_id
,corrected_invoice_dist_id
,corrected_quantity
,country_of_supply
,created_by
,creation_date
,credit_card_trx_id
,daily_amount
,description
,detail_tax_dist_id
,dist_code_combination_id
,dist_match_type
,distribution_class
,distribution_line_number
,earliest_settlement_date
,encumbered_flag
,end_expense_date
,exchange_date
,exchange_rate
,exchange_rate_type
,exchange_rate_variance
,expenditure_item_date
,expenditure_organization_id
,expenditure_type
,expense_group
,extra_po_erv
,final_application_rounding
,final_match_flag
,final_payment_rounding
,final_release_rounding
,fully_paid_acctd_flag
,global_attribute_category
,global_attribute1
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
,global_attribute2
,global_attribute20
,global_attribute3
,global_attribute4
,global_attribute5
,global_attribute6
,global_attribute7
,global_attribute8
,global_attribute9
,gms_burdenable_raw_cost
,historical_flag
,income_tax_region
,intended_use
,inventory_transfer_status
,invoice_distribution_id
,invoice_id
,invoice_includes_prepay_flag
,invoice_line_number,
invoice_price_variance,
je_batch_id,
justification,
last_update_date,
last_update_login,
last_updated_by,
line_group_number,
line_type_lookup_code,
match_status_flag,
matched_uom_lookup_code,
merchant_document_number,
merchant_name,
merchant_reference,
merchant_tax_reg_number,
merchant_taxpayer_id,
mrc_accrual_posted_flag,
mrc_amount,
mrc_amount_to_post,
mrc_base_amount,
mrc_base_amount_to_post,
mrc_base_inv_price_variance,
mrc_cash_je_batch_id,
mrc_cash_posted_flag,
mrc_dist_code_combination_id,
mrc_exchange_date,
mrc_exchange_rate,
mrc_exchange_rate_type,
mrc_exchange_rate_variance,
mrc_je_batch_id,
mrc_posted_amount,
mrc_posted_base_amount,
mrc_posted_flag,
mrc_program_application_id,
mrc_program_id,
mrc_program_update_date,
mrc_rate_var_ccid,
mrc_receipt_conversion_rate,
mrc_request_id,
old_dist_line_number,
old_distribution_id,
org_id,
other_invoice_id,
pa_addition_flag,
pa_cc_ar_invoice_id,
pa_cc_ar_invoice_line_num,
pa_cc_processed_code,
pa_cmt_xface_flag,
pa_quantity,
packet_id,
parent_invoice_id,
parent_reversal_id,
pay_awt_group_id,
period_name,
po_distribution_id,
posted_amount,
posted_base_amount,
posted_flag,
prepay_amount_remaining,
prepay_distribution_id,
prepay_tax_diff_amount,
prepay_tax_parent_id,
price_adjustment_flag,
price_correct_inv_id,
price_correct_qty,
price_var_code_combination_id,
program_application_id,
program_id,
program_update_date,
project_accounting_context,
project_id,
quantity_invoiced,
quantity_unencumbered,
quantity_variance,
rate_var_code_combination_id,
rcv_charge_addition_flag,
rcv_transaction_id,
rec_nrec_rate,
receipt_conversion_rate,
receipt_currency_amount,
receipt_currency_code,
receipt_missing_flag,
receipt_required_flag,
receipt_verified_flag,
recovery_rate_code,
recovery_rate_id,
recovery_rate_name,
recovery_type_code,
recurring_payment_id,
reference_1,
reference_2,
related_id,
related_retainage_dist_id,
release_inv_dist_derived_from,
req_distribution_id,
request_id,
retained_amount_remaining,
retained_invoice_dist_id,
reversal_flag,
root_distribution_id,
rounding_amt,
set_of_books_id,
start_expense_date,
stat_amount,
summary_tax_line_id,
task_id,
tax_already_distributed_flag,
tax_calculated_flag,
tax_code_id,
tax_code_override_flag,
tax_recoverable_flag,
tax_recovery_override_flag,
tax_recovery_rate,
taxable_amount,
taxable_base_amount,
total_dist_amount,
total_dist_base_amount,
type_1099,
unit_price,
upgrade_base_posted_amt,
upgrade_posted_amt,
ussgl_transaction_code,
ussgl_trx_code_context,
vat_code,
web_parameter_id,
withholding_tax_code_id,
xinv_parent_reversal_id,
KCA_OPERATION,
IS_DELETED_FLG,
KCA_SEQ_ID,
kca_seq_date)
(select
accounting_date
,accounting_event_id
,accrual_posted_flag
,accts_pay_code_combination_id
,adjustment_reason
,amount
,amount_at_prepay_pay_xrate
,amount_at_prepay_xrate
,amount_encumbered
,amount_includes_tax_flag
,amount_to_post
,amount_variance
,asset_book_type_code
,asset_category_id
,assets_addition_flag
,assets_tracking_flag
,attribute_category
,attribute1
,attribute10
,attribute11
,attribute12
,attribute13
,attribute14
,attribute15
,attribute2
,attribute3
,attribute4
,attribute5
,attribute6
,attribute7
,attribute8
,attribute9
,award_id
,awt_flag
,awt_gross_amount
,awt_group_id
,awt_invoice_id
,awt_invoice_payment_id
,awt_origin_group_id
,awt_related_id
,awt_tax_rate_id
,AWT_WITHHELD_AMT
,base_amount
,base_amount_encumbered
,base_amount_to_post
,base_amount_variance
,base_invoice_price_variance
,base_quantity_variance
,batch_id
,bc_event_id
,cancellation_flag
,cancelled_flag
,cash_basis_final_app_rounding
,cash_je_batch_id
,cash_posted_flag
,cc_reversal_flag
,charge_applicable_to_dist_id
,company_prepaid_invoice_id
,corrected_invoice_dist_id
,corrected_quantity
,country_of_supply
,created_by
,creation_date
,credit_card_trx_id
,daily_amount
,description
,detail_tax_dist_id
,dist_code_combination_id
,dist_match_type
,distribution_class
,distribution_line_number
,earliest_settlement_date
,encumbered_flag
,end_expense_date
,exchange_date
,exchange_rate
,exchange_rate_type
,exchange_rate_variance
,expenditure_item_date
,expenditure_organization_id
,expenditure_type
,expense_group
,extra_po_erv
,final_application_rounding
,final_match_flag
,final_payment_rounding
,final_release_rounding
,fully_paid_acctd_flag
,global_attribute_category
,global_attribute1
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
,global_attribute2
,global_attribute20
,global_attribute3
,global_attribute4
,global_attribute5
,global_attribute6
,global_attribute7
,global_attribute8
,global_attribute9
,gms_burdenable_raw_cost
,historical_flag
,income_tax_region
,intended_use
,inventory_transfer_status
,invoice_distribution_id
,invoice_id
,invoice_includes_prepay_flag
,invoice_line_number,
invoice_price_variance,
je_batch_id,
justification,
last_update_date,
last_update_login,
last_updated_by,
line_group_number,
line_type_lookup_code,
match_status_flag,
matched_uom_lookup_code,
merchant_document_number,
merchant_name,
merchant_reference,
merchant_tax_reg_number,
merchant_taxpayer_id,
mrc_accrual_posted_flag,
mrc_amount,
mrc_amount_to_post,
mrc_base_amount,
mrc_base_amount_to_post,
mrc_base_inv_price_variance,
mrc_cash_je_batch_id,
mrc_cash_posted_flag,
mrc_dist_code_combination_id,
mrc_exchange_date,
mrc_exchange_rate,
mrc_exchange_rate_type,
mrc_exchange_rate_variance,
mrc_je_batch_id,
mrc_posted_amount,
mrc_posted_base_amount,
mrc_posted_flag,
mrc_program_application_id,
mrc_program_id,
mrc_program_update_date,
mrc_rate_var_ccid,
mrc_receipt_conversion_rate,
mrc_request_id,
old_dist_line_number,
old_distribution_id,
org_id,
other_invoice_id,
pa_addition_flag,
pa_cc_ar_invoice_id,
pa_cc_ar_invoice_line_num,
pa_cc_processed_code,
pa_cmt_xface_flag,
pa_quantity,
packet_id,
parent_invoice_id,
parent_reversal_id,
pay_awt_group_id,
period_name,
po_distribution_id,
posted_amount,
posted_base_amount,
posted_flag,
prepay_amount_remaining,
prepay_distribution_id,
prepay_tax_diff_amount,
prepay_tax_parent_id,
price_adjustment_flag,
price_correct_inv_id,
price_correct_qty,
price_var_code_combination_id,
program_application_id,
program_id,
program_update_date,
project_accounting_context,
project_id,
quantity_invoiced,
quantity_unencumbered,
quantity_variance,
rate_var_code_combination_id,
rcv_charge_addition_flag,
rcv_transaction_id,
rec_nrec_rate,
receipt_conversion_rate,
receipt_currency_amount,
receipt_currency_code,
receipt_missing_flag,
receipt_required_flag,
receipt_verified_flag,
recovery_rate_code,
recovery_rate_id,
recovery_rate_name,
recovery_type_code,
recurring_payment_id,
reference_1,
reference_2,
related_id,
related_retainage_dist_id,
release_inv_dist_derived_from,
req_distribution_id,
request_id,
retained_amount_remaining,
retained_invoice_dist_id,
reversal_flag,
root_distribution_id,
rounding_amt,
set_of_books_id,
start_expense_date,
stat_amount,
summary_tax_line_id,
task_id,
tax_already_distributed_flag,
tax_calculated_flag,
tax_code_id,
tax_code_override_flag,
tax_recoverable_flag,
tax_recovery_override_flag,
tax_recovery_rate,
taxable_amount,
taxable_base_amount,
total_dist_amount,
total_dist_base_amount,
type_1099,
unit_price,
upgrade_base_posted_amt,
upgrade_posted_amt,
ussgl_transaction_code,
ussgl_trx_code_context,
vat_code,
web_parameter_id,
withholding_tax_code_id,
xinv_parent_reversal_id,
KCA_OPERATION,
'N' as IS_DELETED_FLG,
cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
kca_seq_date
from bec_ods_stg.ap_invoice_distributions_all);

end;


update bec_etl_ctrl.batch_ods_info set load_type = 'I', last_refresh_date = getdate() where ods_table_name='ap_invoice_distributions_all';

commit;