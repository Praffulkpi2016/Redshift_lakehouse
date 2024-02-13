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

DROP TABLE IF EXISTS bec_ods.ap_system_parameters_all;

CREATE TABLE if NOT EXISTS bec_ods.ap_system_parameters_all
(
	last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,base_currency_code VARCHAR(15)   ENCODE lzo
	,recalc_pay_schedule_flag VARCHAR(1)   ENCODE lzo
	,auto_calculate_interest_flag VARCHAR(1)   ENCODE lzo
	,invoice_currency_code VARCHAR(15)   ENCODE lzo
	,payment_currency_code VARCHAR(15)   ENCODE lzo
	,invoice_net_gross_flag VARCHAR(1)   ENCODE lzo
	,bank_account_id NUMERIC(15,0)   ENCODE az64
	,check_overflow_lookup_code VARCHAR(25)   ENCODE lzo
	,batch_control_flag VARCHAR(1)   ENCODE lzo
	,terms_id NUMERIC(15,0)   ENCODE az64
	,always_take_disc_flag VARCHAR(1)   ENCODE lzo
	,pay_date_basis_lookup_code VARCHAR(25)   ENCODE lzo
	,accts_pay_code_combination_id NUMERIC(15,0)   ENCODE az64
	,sales_tax_code_combination_id NUMERIC(15,0)   ENCODE az64
	,disc_lost_code_combination_id NUMERIC(15,0)   ENCODE az64
	,disc_taken_code_combination_id NUMERIC(15,0)   ENCODE az64
	,hold_gain_code_combination_id NUMERIC(15,0)   ENCODE az64
	,trans_gain_code_combination_id NUMERIC(15,0)   ENCODE az64
	,apply_advances_default VARCHAR(4)   ENCODE lzo
	,add_days_settlement_date NUMERIC(15,0)   ENCODE az64
	,cost_of_money NUMERIC(28,10)   ENCODE az64
	,days_between_check_cycles NUMERIC(15,0)   ENCODE az64
	,federal_identification_num VARCHAR(20)   ENCODE lzo
	,location_id NUMERIC(15,0)   ENCODE az64
	,create_employee_vendor_flag VARCHAR(1)   ENCODE lzo
	,employee_terms_id NUMERIC(15,0)   ENCODE az64
	,employee_pay_group_lookup_code VARCHAR(25)   ENCODE lzo
	,employee_payment_priority NUMERIC(2,0)   ENCODE az64
	,prepay_code_combination_id NUMERIC(15,0)   ENCODE az64
	,confirm_date_as_inv_num_flag VARCHAR(1)   ENCODE lzo
	,update_pay_site_flag VARCHAR(1)   ENCODE lzo
	,default_exchange_rate_type VARCHAR(30)   ENCODE lzo
	,gain_code_combination_id NUMERIC(15,0)   ENCODE az64
	,loss_code_combination_id NUMERIC(15,0)   ENCODE az64
	,make_rate_mandatory_flag VARCHAR(1)   ENCODE lzo
	,multi_currency_flag VARCHAR(1)   ENCODE lzo
	,gl_date_from_receipt_flag VARCHAR(25)   ENCODE lzo
	,disc_is_inv_less_tax_flag VARCHAR(1)   ENCODE lzo
	,match_on_tax_flag VARCHAR(1)   ENCODE lzo
	,accounting_method_option VARCHAR(25)   ENCODE lzo
	,expense_post_option VARCHAR(25)   ENCODE lzo
	,discount_taken_post_option VARCHAR(25)   ENCODE lzo
	,gain_loss_post_option VARCHAR(25)   ENCODE lzo
	,cash_post_option VARCHAR(25)   ENCODE lzo
	,future_pay_post_option VARCHAR(25)   ENCODE lzo
	,date_format_lookup_code VARCHAR(25)   ENCODE lzo
	,replace_check_flag VARCHAR(1)   ENCODE lzo
	,online_print_flag VARCHAR(1)   ENCODE lzo
	,eft_user_number VARCHAR(30)   ENCODE lzo
	,max_outlay NUMERIC(28,10)   ENCODE az64
	,vendor_pay_group_lookup_code VARCHAR(25)   ENCODE lzo
	,require_tax_entry_flag VARCHAR(1)   ENCODE lzo
	,approvals_option VARCHAR(25)   ENCODE lzo
	,post_dated_payments_flag VARCHAR(1)   ENCODE lzo
	,secondary_accounting_method VARCHAR(25)   ENCODE lzo
	,secondary_set_of_books_id NUMERIC(15,0)   ENCODE az64
	,take_vat_before_discount_flag VARCHAR(1)   ENCODE lzo
	,interest_tolerance_amount NUMERIC(28,10)   ENCODE az64
	,interest_code_combination_id NUMERIC(15,0)   ENCODE az64
	,terms_date_basis VARCHAR(25)   ENCODE lzo
	,allow_future_pay_flag VARCHAR(1)   ENCODE lzo
	,auto_tax_calc_flag VARCHAR(1)   ENCODE lzo
	,automatic_offsets_flag VARCHAR(1)   ENCODE lzo
	,liability_post_lookup_code VARCHAR(25)   ENCODE lzo
	,interest_accts_pay_ccid NUMERIC(15,0)   ENCODE az64
	,liability_post_option VARCHAR(25)   ENCODE lzo
	,discount_distribution_method VARCHAR(25)   ENCODE lzo
	,rate_var_code_combination_id NUMERIC(15,0)   ENCODE az64
	,combined_filing_flag VARCHAR(1)   ENCODE lzo
	,income_tax_region VARCHAR(10)   ENCODE lzo
	,income_tax_region_flag VARCHAR(1)   ENCODE lzo
	,hold_unmatched_invoices_flag VARCHAR(1)   ENCODE lzo
	,allow_dist_match_flag VARCHAR(1)   ENCODE lzo
	,allow_final_match_flag VARCHAR(1)   ENCODE lzo
	,allow_flex_override_flag VARCHAR(1)   ENCODE lzo
	,allow_paid_invoice_adjust VARCHAR(1)   ENCODE lzo
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,ussgl_trx_code_context VARCHAR(30)   ENCODE lzo
	,inv_doc_category_override VARCHAR(1)   ENCODE lzo
	,pay_doc_category_override VARCHAR(1)   ENCODE lzo
	,vendor_auto_int_default VARCHAR(1)   ENCODE lzo
	,summary_journals_default VARCHAR(1)   ENCODE lzo
	,rate_var_gain_ccid NUMERIC(15,0)   ENCODE az64
	,rate_var_loss_ccid NUMERIC(15,0)   ENCODE az64
	,transfer_desc_flex_flag VARCHAR(1)   ENCODE lzo
	,allow_awt_flag VARCHAR(1)   ENCODE lzo
	,default_awt_group_id NUMERIC(15,0)   ENCODE az64
	,allow_awt_override VARCHAR(1)   ENCODE lzo
	,create_awt_dists_type VARCHAR(25)   ENCODE lzo
	,create_awt_invoices_type VARCHAR(25)   ENCODE lzo
	,awt_include_discount_amt VARCHAR(1)   ENCODE lzo
	,awt_include_tax_amt VARCHAR(1)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,recon_accounting_flag VARCHAR(1)   ENCODE lzo
	,auto_create_freight_flag VARCHAR(1)   ENCODE lzo
	,freight_code_combination_id NUMERIC(15,0)   ENCODE az64
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
	,allow_supplier_bank_override VARCHAR(1)   ENCODE lzo
	,use_multiple_supplier_banks VARCHAR(1)   ENCODE lzo
	,auto_tax_calc_override VARCHAR(1)   ENCODE lzo
	,amount_includes_tax_flag VARCHAR(1)   ENCODE lzo
	,amount_includes_tax_override VARCHAR(1)   ENCODE lzo
	,vat_code VARCHAR(15)   ENCODE lzo
	,use_bank_charge_flag VARCHAR(1)   ENCODE lzo
	,bank_charge_bearer VARCHAR(1)   ENCODE lzo
	,rounding_error_ccid NUMERIC(15,0)   ENCODE az64
	,rounding_error_post_option VARCHAR(25)   ENCODE lzo
	,tax_from_po_flag VARCHAR(1)   ENCODE lzo
	,tax_from_vendor_site_flag VARCHAR(1)   ENCODE lzo
	,tax_from_vendor_flag VARCHAR(1)   ENCODE lzo
	,tax_from_account_flag VARCHAR(1)   ENCODE lzo
	,tax_from_system_flag VARCHAR(1)   ENCODE lzo
	,tax_from_inv_header_flag VARCHAR(1)   ENCODE lzo
	,tax_from_template_flag VARCHAR(1)   ENCODE lzo
	,tax_hier_po_shipment NUMERIC(28,10)   ENCODE az64
	,tax_hier_vendor NUMERIC(28,10)   ENCODE az64
	,tax_hier_vendor_site NUMERIC(28,10)   ENCODE az64
	,tax_hier_account NUMERIC(28,10)   ENCODE az64
	,tax_hier_system NUMERIC(28,10)   ENCODE az64
	,tax_hier_invoice NUMERIC(28,10)   ENCODE az64
	,tax_hier_template NUMERIC(28,10)   ENCODE az64
	,enforce_tax_from_account VARCHAR(1)   ENCODE lzo
	,mrc_base_currency_code VARCHAR(2000)   ENCODE lzo
	,mrc_secondary_set_of_books_id VARCHAR(2000)   ENCODE lzo
	,match_option VARCHAR(25)   ENCODE lzo
	,gain_loss_calc_level VARCHAR(30)   ENCODE lzo
	,when_to_account_pmt VARCHAR(30)   ENCODE lzo
	,when_to_account_gain_loss VARCHAR(30)   ENCODE lzo
	,future_dated_pmt_acct_source VARCHAR(30)   ENCODE lzo
	,future_dated_pmt_liab_relief VARCHAR(30)   ENCODE lzo
	,gl_transfer_allow_override VARCHAR(1)   ENCODE lzo
	,gl_transfer_process_days NUMERIC(15,0)   ENCODE az64
	,gl_transfer_mode VARCHAR(1)   ENCODE lzo
	,gl_transfer_submit_journal_imp VARCHAR(1)   ENCODE lzo
	,include_reporting_sob VARCHAR(1)   ENCODE lzo
	,expense_report_id NUMERIC(15,0)   ENCODE az64
	,prepayment_terms_id NUMERIC(15,0)   ENCODE az64
	,calc_user_xrate VARCHAR(1)   ENCODE lzo
	,sort_by_alternate_field VARCHAR(1)   ENCODE lzo
	,approval_workflow_flag VARCHAR(1)   ENCODE lzo
	,allow_force_approval_flag VARCHAR(1)   ENCODE lzo
	,validate_before_approval_flag VARCHAR(1)   ENCODE lzo
	,xml_payments_auto_confirm_flag VARCHAR(1)   ENCODE lzo
	,prorate_int_inv_across_dists VARCHAR(1)   ENCODE lzo
	,build_prepayment_accounts_flag VARCHAR(1)   ENCODE lzo
	,enable_1099_on_awt_flag VARCHAR(1)   ENCODE lzo
	,stop_prepay_across_bal_flag VARCHAR(1)   ENCODE lzo
	,automatic_offsets_change_flag VARCHAR(1)   ENCODE lzo
	,tolerance_id NUMERIC(15,0)   ENCODE az64
	,tax_tolerance NUMERIC(28,10)   ENCODE az64
	,tax_tol_amt_range NUMERIC(28,10)   ENCODE az64
	,services_tolerance_id NUMERIC(15,0)   ENCODE az64
	,prepay_tax_diff_ccid NUMERIC(15,0)   ENCODE az64
	,ce_bank_acct_use_id NUMERIC(15,0)   ENCODE az64
	,approval_timing VARCHAR(30)   ENCODE lzo
	,receipt_acceptance_days NUMERIC(15,0)   ENCODE az64
	,allow_inv_third_party_ovrd VARCHAR(1)   ENCODE lzo
	,allow_pymt_third_party_ovrd VARCHAR(1)   ENCODE lzo
    ,WITHHOLDING_DATE_BASIS VARCHAR(20)   ENCODE lzo
    ,INVRATE_FOR_PREPAY_TAX VARCHAR(1) ENCODE lzo	
    ,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.ap_system_parameters_all (
        last_update_date,
        last_updated_by,
        set_of_books_id,
        base_currency_code,
        recalc_pay_schedule_flag,
        auto_calculate_interest_flag,
        invoice_currency_code,
        payment_currency_code,
        invoice_net_gross_flag,
        bank_account_id,
        check_overflow_lookup_code,
        batch_control_flag,
        terms_id,
        always_take_disc_flag,
        pay_date_basis_lookup_code,
        accts_pay_code_combination_id,
        sales_tax_code_combination_id,
        disc_lost_code_combination_id,
        disc_taken_code_combination_id,
        hold_gain_code_combination_id,
        trans_gain_code_combination_id,
        apply_advances_default,
        add_days_settlement_date,
        cost_of_money,
        days_between_check_cycles,
        federal_identification_num,
        location_id,
        create_employee_vendor_flag,
        employee_terms_id,
        employee_pay_group_lookup_code,
        employee_payment_priority,
        prepay_code_combination_id,
        confirm_date_as_inv_num_flag,
        update_pay_site_flag,
        default_exchange_rate_type,
        gain_code_combination_id,
        loss_code_combination_id,
        make_rate_mandatory_flag,
        multi_currency_flag,
        gl_date_from_receipt_flag,
        disc_is_inv_less_tax_flag,
        match_on_tax_flag,
        accounting_method_option,
        expense_post_option,
        discount_taken_post_option,
        gain_loss_post_option,
        cash_post_option,
        future_pay_post_option,
        date_format_lookup_code,
        replace_check_flag,
        online_print_flag,
        eft_user_number,
        max_outlay,
        vendor_pay_group_lookup_code,
        require_tax_entry_flag,
        approvals_option,
        post_dated_payments_flag,
        secondary_accounting_method,
        secondary_set_of_books_id,
        take_vat_before_discount_flag,
        interest_tolerance_amount,
        interest_code_combination_id,
        terms_date_basis,
        allow_future_pay_flag,
        auto_tax_calc_flag,
        automatic_offsets_flag,
        liability_post_lookup_code,
        interest_accts_pay_ccid,
        liability_post_option,
        discount_distribution_method,
        rate_var_code_combination_id,
        combined_filing_flag,
        income_tax_region,
        income_tax_region_flag,
        hold_unmatched_invoices_flag,
        allow_dist_match_flag,
        allow_final_match_flag,
        allow_flex_override_flag,
        allow_paid_invoice_adjust,
        ussgl_transaction_code,
        ussgl_trx_code_context,
        inv_doc_category_override,
        pay_doc_category_override,
        vendor_auto_int_default,
        summary_journals_default,
        rate_var_gain_ccid,
        rate_var_loss_ccid,
        transfer_desc_flex_flag,
        allow_awt_flag,
        default_awt_group_id,
        allow_awt_override,
        create_awt_dists_type,
        create_awt_invoices_type,
        awt_include_discount_amt,
        awt_include_tax_amt,
        org_id,
        recon_accounting_flag,
        auto_create_freight_flag,
        freight_code_combination_id,
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
        allow_supplier_bank_override,
        use_multiple_supplier_banks,
      --  ap_tax_rounding_rule,
        auto_tax_calc_override,
        amount_includes_tax_flag,
        amount_includes_tax_override,
        vat_code, 
        use_bank_charge_flag,
        bank_charge_bearer,
        rounding_error_ccid,
        rounding_error_post_option,
        tax_from_po_flag,
        tax_from_vendor_site_flag,
        tax_from_vendor_flag,
        tax_from_account_flag,
        tax_from_system_flag,
        tax_from_inv_header_flag,
        tax_from_template_flag,
        tax_hier_po_shipment,
        tax_hier_vendor,
        tax_hier_vendor_site,
        tax_hier_account,
        tax_hier_system,
        tax_hier_invoice,
        tax_hier_template,
        enforce_tax_from_account,
        mrc_base_currency_code,
        mrc_secondary_set_of_books_id,
        match_option,
        gain_loss_calc_level,
        when_to_account_pmt,
        when_to_account_gain_loss,
        future_dated_pmt_acct_source,
        future_dated_pmt_liab_relief,
        gl_transfer_allow_override,
        gl_transfer_process_days,
        gl_transfer_mode,
        gl_transfer_submit_journal_imp,
        include_reporting_sob,
        expense_report_id,
        prepayment_terms_id,
        calc_user_xrate,
        sort_by_alternate_field,
        approval_workflow_flag,
        allow_force_approval_flag,
        validate_before_approval_flag,
        xml_payments_auto_confirm_flag,
        prorate_int_inv_across_dists,
        build_prepayment_accounts_flag,
        enable_1099_on_awt_flag,
        stop_prepay_across_bal_flag,
        automatic_offsets_change_flag,
        tolerance_id,
        tax_tolerance,
        tax_tol_amt_range,
        services_tolerance_id,
        prepay_tax_diff_ccid,
        ce_bank_acct_use_id,
        approval_timing,
        receipt_acceptance_days,
        allow_inv_third_party_ovrd,
        allow_pymt_third_party_ovrd,
		WITHHOLDING_DATE_BASIS,
		INVRATE_FOR_PREPAY_TAX,
		KCA_OPERATION,
		IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date
)
    (SELECT
        last_update_date,
        last_updated_by,
        set_of_books_id,
        base_currency_code,
        recalc_pay_schedule_flag,
        auto_calculate_interest_flag,
        invoice_currency_code,
        payment_currency_code,
        invoice_net_gross_flag,
        bank_account_id,
        check_overflow_lookup_code,
        batch_control_flag,
        terms_id,
        always_take_disc_flag,
        pay_date_basis_lookup_code,
        accts_pay_code_combination_id,
        sales_tax_code_combination_id,
        disc_lost_code_combination_id,
        disc_taken_code_combination_id,
        hold_gain_code_combination_id,
        trans_gain_code_combination_id,
        apply_advances_default,
        add_days_settlement_date,
        cost_of_money,
        days_between_check_cycles,
        federal_identification_num,
        location_id,
        create_employee_vendor_flag,
        employee_terms_id,
        employee_pay_group_lookup_code,
        employee_payment_priority,
        prepay_code_combination_id,
        confirm_date_as_inv_num_flag,
        update_pay_site_flag,
        default_exchange_rate_type,
        gain_code_combination_id,
        loss_code_combination_id,
        make_rate_mandatory_flag,
        multi_currency_flag,
        gl_date_from_receipt_flag,
        disc_is_inv_less_tax_flag,
        match_on_tax_flag,
        accounting_method_option,
        expense_post_option,
        discount_taken_post_option,
        gain_loss_post_option,
        cash_post_option,
        future_pay_post_option,
        date_format_lookup_code,
        replace_check_flag,
        online_print_flag,
        eft_user_number,
        max_outlay,
        vendor_pay_group_lookup_code,
        require_tax_entry_flag,
        approvals_option,
        post_dated_payments_flag,
        secondary_accounting_method,
        secondary_set_of_books_id,
        take_vat_before_discount_flag,
        interest_tolerance_amount,
        interest_code_combination_id,
        terms_date_basis,
        allow_future_pay_flag,
        auto_tax_calc_flag,
        automatic_offsets_flag,
        liability_post_lookup_code,
        interest_accts_pay_ccid,
        liability_post_option,
        discount_distribution_method,
        rate_var_code_combination_id,
        combined_filing_flag,
        income_tax_region,
        income_tax_region_flag,
        hold_unmatched_invoices_flag,
        allow_dist_match_flag,
        allow_final_match_flag,
        allow_flex_override_flag,
        allow_paid_invoice_adjust,
        ussgl_transaction_code,
        ussgl_trx_code_context,
        inv_doc_category_override,
        pay_doc_category_override,
        vendor_auto_int_default,
        summary_journals_default,
        rate_var_gain_ccid,
        rate_var_loss_ccid,
        transfer_desc_flex_flag,
        allow_awt_flag,
        default_awt_group_id,
        allow_awt_override,
        create_awt_dists_type,
        create_awt_invoices_type,
        awt_include_discount_amt,
        awt_include_tax_amt,
        org_id,
        recon_accounting_flag,
        auto_create_freight_flag,
        freight_code_combination_id,
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
        allow_supplier_bank_override,
        use_multiple_supplier_banks, 
        auto_tax_calc_override,
        amount_includes_tax_flag,
        amount_includes_tax_override,
        vat_code, 
        use_bank_charge_flag,
        bank_charge_bearer,
        rounding_error_ccid,
        rounding_error_post_option,
        tax_from_po_flag,
        tax_from_vendor_site_flag,
        tax_from_vendor_flag,
        tax_from_account_flag,
        tax_from_system_flag,
        tax_from_inv_header_flag,
        tax_from_template_flag,
        tax_hier_po_shipment,
        tax_hier_vendor,
        tax_hier_vendor_site,
        tax_hier_account,
        tax_hier_system,
        tax_hier_invoice,
        tax_hier_template,
        enforce_tax_from_account,
        mrc_base_currency_code,
        mrc_secondary_set_of_books_id,
        match_option,
        gain_loss_calc_level,
        when_to_account_pmt,
        when_to_account_gain_loss,
        future_dated_pmt_acct_source,
        future_dated_pmt_liab_relief,
        gl_transfer_allow_override,
        gl_transfer_process_days,
        gl_transfer_mode,
        gl_transfer_submit_journal_imp,
        include_reporting_sob,
        expense_report_id,
        prepayment_terms_id,
        calc_user_xrate,
        sort_by_alternate_field,
        approval_workflow_flag,
        allow_force_approval_flag,
        validate_before_approval_flag,
        xml_payments_auto_confirm_flag,
        prorate_int_inv_across_dists,
        build_prepayment_accounts_flag,
        enable_1099_on_awt_flag,
        stop_prepay_across_bal_flag,
        automatic_offsets_change_flag,
        tolerance_id,
        tax_tolerance,
        tax_tol_amt_range,
        services_tolerance_id,
        prepay_tax_diff_ccid,
        ce_bank_acct_use_id,
        approval_timing,
        receipt_acceptance_days,
        allow_inv_third_party_ovrd,
        allow_pymt_third_party_ovrd,
		WITHHOLDING_DATE_BASIS,
		INVRATE_FOR_PREPAY_TAX,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
    FROM
        bec_ods_stg.ap_system_parameters_all);
		
end;
 

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_system_parameters_all'; 
	
commit;