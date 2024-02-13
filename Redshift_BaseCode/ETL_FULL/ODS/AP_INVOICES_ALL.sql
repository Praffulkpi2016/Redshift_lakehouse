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

drop TABLE if exists bec_ods.ap_invoices_all;

CREATE TABLE IF NOT EXISTS bec_ods.ap_invoices_all
(
	pay_curr_invoice_amount NUMERIC(28,10)   ENCODE az64
	,mrc_base_amount VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_rate VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_rate_type VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_date VARCHAR(2000)   ENCODE lzo
	,gl_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,award_id NUMERIC(15,0)   ENCODE az64
	,paid_on_behalf_employee_id NUMERIC(15,0)   ENCODE az64
	,amt_due_ccard_company NUMERIC(28,10)   ENCODE az64
	,amt_due_employee NUMERIC(28,10)   ENCODE az64
	,approval_ready_flag VARCHAR(1)   ENCODE lzo
	,approval_iteration NUMERIC(9,0)   ENCODE az64
	,wfapproval_status VARCHAR(50)   ENCODE lzo
	,requester_id NUMERIC(15,0)   ENCODE az64
	,validation_request_id NUMERIC(15,0)   ENCODE az64
	,validated_tax_amount NUMERIC(28,10)   ENCODE az64
	,quick_credit VARCHAR(1)   ENCODE lzo
	,credited_invoice_id NUMERIC(15,0)   ENCODE az64
	,distribution_set_id NUMERIC(15,0)   ENCODE az64
	,application_id NUMERIC(15,0)   ENCODE az64
	,product_table VARCHAR(30)   ENCODE lzo
	,reference_key1 VARCHAR(150)   ENCODE lzo
	,reference_key2 VARCHAR(150)   ENCODE lzo
	,reference_key3 VARCHAR(150)   ENCODE lzo
	,reference_key4 VARCHAR(150)   ENCODE lzo
	,reference_key5 VARCHAR(150)   ENCODE lzo
	,total_tax_amount NUMERIC(28,10)   ENCODE az64
	,self_assessed_tax_amount NUMERIC(28,10)   ENCODE az64
	,tax_related_invoice_id NUMERIC(15,0)   ENCODE az64
	,trx_business_category VARCHAR(240)   ENCODE lzo
	,user_defined_fisc_class VARCHAR(240)   ENCODE lzo
	,taxation_country VARCHAR(30)   ENCODE lzo
	,document_sub_type VARCHAR(150)   ENCODE lzo
	,supplier_tax_invoice_number VARCHAR(150)   ENCODE lzo
	,supplier_tax_invoice_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,supplier_tax_exchange_rate NUMERIC(28,10)   ENCODE az64
	,tax_invoice_recording_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,tax_invoice_internal_seq VARCHAR(150)   ENCODE lzo
	,legal_entity_id NUMERIC(15,0)   ENCODE az64
	,historical_flag VARCHAR(1)   ENCODE lzo
	,force_revalidation_flag VARCHAR(1)   ENCODE lzo
	,bank_charge_bearer VARCHAR(30)   ENCODE lzo
	,remittance_message1 VARCHAR(150)   ENCODE lzo
	,remittance_message2 VARCHAR(150)   ENCODE lzo
	,remittance_message3 VARCHAR(150)   ENCODE lzo
	,unique_remittance_identifier VARCHAR(30)   ENCODE lzo
	,uri_check_digit VARCHAR(2)   ENCODE lzo
	,settlement_priority VARCHAR(30)   ENCODE lzo
	,payment_reason_code VARCHAR(30)   ENCODE lzo
	,payment_reason_comments VARCHAR(240)   ENCODE lzo
	,payment_method_code VARCHAR(30)   ENCODE lzo
	,delivery_channel_code VARCHAR(30)   ENCODE lzo
	,quick_po_header_id NUMERIC(15,0)   ENCODE az64
	,net_of_retainage_flag VARCHAR(1)   ENCODE lzo
	,release_amount_net_of_tax NUMERIC(28,10)   ENCODE az64
	,control_amount NUMERIC(28,10)   ENCODE az64
	,party_id NUMERIC(15,0)   ENCODE az64
	,party_site_id NUMERIC(15,0)   ENCODE az64
	,pay_proc_trxn_type_code VARCHAR(30)   ENCODE lzo
	,payment_function VARCHAR(30)   ENCODE lzo
	,cust_registration_code VARCHAR(50)   ENCODE lzo
	,cust_registration_number VARCHAR(30)   ENCODE lzo
	,port_of_entry_code VARCHAR(30)   ENCODE lzo
	,external_bank_account_id NUMERIC(15,0)   ENCODE az64
	,vendor_contact_id NUMERIC(15,0)   ENCODE az64
	,internal_contact_email VARCHAR(2000)   ENCODE lzo
	,disc_is_inv_less_tax_flag VARCHAR(1)   ENCODE lzo
	,exclude_freight_from_discount VARCHAR(1)   ENCODE lzo
	,pay_awt_group_id NUMERIC(15,0)   ENCODE az64
	,original_invoice_amount NUMERIC(28,10)   ENCODE az64
	,dispute_reason VARCHAR(100)   ENCODE lzo
	,remit_to_supplier_name VARCHAR(240)   ENCODE lzo
	,remit_to_supplier_id NUMERIC(15,0)   ENCODE az64
	,remit_to_supplier_site VARCHAR(240)   ENCODE lzo
	,remit_to_supplier_site_id NUMERIC(15,0)   ENCODE az64
	,relationship_id NUMERIC(15,0)   ENCODE az64
	,payment_cross_rate NUMERIC(28,10)   ENCODE az64
	,invoice_amount NUMERIC(28,10)   ENCODE az64
	,vendor_site_id NUMERIC(15,0)   ENCODE az64
	,amount_paid NUMERIC(28,10)   ENCODE az64
	,discount_amount_taken NUMERIC(28,10)   ENCODE az64
	,invoice_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,source VARCHAR(25)   ENCODE lzo
	,invoice_type_lookup_code VARCHAR(25)   ENCODE lzo
	,description VARCHAR(240)   ENCODE lzo
	,batch_id NUMERIC(15,0)   ENCODE az64
	,amount_applicable_to_discount NUMERIC(28,10)   ENCODE az64
	,tax_amount NUMERIC(28,10)   ENCODE az64
	,terms_id NUMERIC(15,0)   ENCODE az64
	,terms_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,payment_method_lookup_code VARCHAR(25)   ENCODE lzo
	,pay_group_lookup_code VARCHAR(25)   ENCODE lzo
	,accts_pay_code_combination_id NUMERIC(15,0)   ENCODE az64
	,payment_status_flag VARCHAR(1)   ENCODE lzo
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,base_amount NUMERIC(28,10)   ENCODE az64
	,vat_code VARCHAR(15)   ENCODE lzo
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,exclusive_payment_flag VARCHAR(1)   ENCODE lzo
	,po_header_id NUMERIC(15,0)   ENCODE az64
	,freight_amount NUMERIC(28,10)   ENCODE az64
	,goods_received_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,invoice_received_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,voucher_num VARCHAR(50)   ENCODE lzo
	,approved_amount NUMERIC(28,10)   ENCODE az64
	,recurring_payment_id NUMERIC(15,0)   ENCODE az64
	,exchange_rate NUMERIC(28,10)   ENCODE az64
	,exchange_rate_type VARCHAR(30)   ENCODE lzo
	,exchange_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,earliest_settlement_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,original_prepayment_amount NUMERIC(28,10)   ENCODE az64
	,doc_sequence_id NUMERIC(15,0)   ENCODE az64
	,doc_sequence_value NUMERIC(15,0)   ENCODE az64
	,doc_category_code VARCHAR(30)   ENCODE lzo
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
	,attribute_category VARCHAR(150)   ENCODE lzo
	,approval_status VARCHAR(25)   ENCODE lzo
	,approval_description VARCHAR(240)   ENCODE lzo
	,invoice_distribution_total NUMERIC(28,10)   ENCODE az64
	,posting_status VARCHAR(15)   ENCODE lzo
	,prepay_flag VARCHAR(1)   ENCODE lzo
	,authorized_by VARCHAR(25)   ENCODE lzo
	,cancelled_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cancelled_by NUMERIC(15,0)   ENCODE az64
	,cancelled_amount NUMERIC(28,10)   ENCODE az64
	,temp_cancelled_amount NUMERIC(28,10)   ENCODE az64
	,project_accounting_context VARCHAR(30)   ENCODE lzo
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,ussgl_trx_code_context VARCHAR(30)   ENCODE lzo
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,expenditure_type VARCHAR(30)   ENCODE lzo
	,expenditure_item_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,pa_quantity NUMERIC(28,10)   ENCODE az64
	,expenditure_organization_id NUMERIC(15,0)   ENCODE az64
	,pa_default_dist_ccid NUMERIC(15,0)   ENCODE az64
	,vendor_prepay_amount NUMERIC(28,10)   ENCODE az64
	,payment_amount_total NUMERIC(28,10)   ENCODE az64
	,awt_flag VARCHAR(1)   ENCODE lzo
	,awt_group_id NUMERIC(15,0)   ENCODE az64
	,reference_1 VARCHAR(30)   ENCODE lzo
	,reference_2 VARCHAR(30)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,pre_withholding_amount NUMERIC(28,10)   ENCODE az64
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
	,auto_tax_calc_flag VARCHAR(1)   ENCODE lzo
	,payment_cross_rate_type VARCHAR(30)   ENCODE lzo
	,payment_cross_rate_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,invoice_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,invoice_num VARCHAR(50)   ENCODE lzo
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,invoice_currency_code VARCHAR(15)   ENCODE lzo
	,payment_currency_code VARCHAR(15)   ENCODE lzo
	,PO_MATCHED_FLAG VARCHAR(1)   ENCODE lzo
	,VALIDATION_WORKER_ID  NUMERIC(15,0)   ENCODE az64
	,"PAY_GROUP_LOOKUP_CODE#1" VARCHAR(30)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;

INSERT INTO bec_ods.ap_invoices_all
	    (pay_curr_invoice_amount,
        mrc_base_amount,
        mrc_exchange_rate,
        mrc_exchange_rate_type,
        mrc_exchange_date,
        gl_date,
        award_id,
        paid_on_behalf_employee_id,
        amt_due_ccard_company,
        amt_due_employee,
        approval_ready_flag,
        approval_iteration,
        wfapproval_status,
        requester_id,
        validation_request_id,
        validated_tax_amount,
        quick_credit,
        credited_invoice_id,
        distribution_set_id,
        application_id,
        product_table,
        reference_key1,
        reference_key2,
        reference_key3,
        reference_key4,
        reference_key5,
        total_tax_amount,
        self_assessed_tax_amount,
        tax_related_invoice_id,
        trx_business_category,
        user_defined_fisc_class,
        taxation_country,
        document_sub_type,
        supplier_tax_invoice_number,
        supplier_tax_invoice_date,
        supplier_tax_exchange_rate,
        tax_invoice_recording_date,
        tax_invoice_internal_seq,
        legal_entity_id,
        historical_flag,
        force_revalidation_flag,
        bank_charge_bearer,
        remittance_message1,
        remittance_message2,
        remittance_message3,
        unique_remittance_identifier,
        uri_check_digit,
        settlement_priority,
        payment_reason_code,
        payment_reason_comments,
        payment_method_code,
        delivery_channel_code,
        quick_po_header_id,
        net_of_retainage_flag,
        release_amount_net_of_tax,
        control_amount,
        party_id,
        party_site_id,
        pay_proc_trxn_type_code,
        payment_function,
        cust_registration_code,
        cust_registration_number,
        port_of_entry_code,
        external_bank_account_id,
        vendor_contact_id,
        internal_contact_email,
        disc_is_inv_less_tax_flag,
        exclude_freight_from_discount,
        pay_awt_group_id,
        original_invoice_amount,
        dispute_reason,
        remit_to_supplier_name,
        remit_to_supplier_id,
        remit_to_supplier_site,
        remit_to_supplier_site_id,
        relationship_id,
        payment_cross_rate,
        invoice_amount,
        vendor_site_id,
        amount_paid,
        discount_amount_taken,
        invoice_date,
        source,
        invoice_type_lookup_code,
        description,
        batch_id,
        amount_applicable_to_discount,
        tax_amount,
        terms_id,
        terms_date,
        payment_method_lookup_code,
        pay_group_lookup_code,
        accts_pay_code_combination_id,
        payment_status_flag,
        creation_date,
        created_by,
        base_amount,
        vat_code,
        last_update_login,
        exclusive_payment_flag,
        po_header_id,
        freight_amount,
        goods_received_date,
        invoice_received_date,
        voucher_num,
        approved_amount,
        recurring_payment_id,
        exchange_rate,
        exchange_rate_type,
        exchange_date,
        earliest_settlement_date,
        original_prepayment_amount,
        doc_sequence_id,
        doc_sequence_value,
        doc_category_code,
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
        attribute_category,
        approval_status,
        approval_description,
        invoice_distribution_total,
        posting_status,
        prepay_flag,
        authorized_by,
        cancelled_date,
        cancelled_by,
        cancelled_amount,
        temp_cancelled_amount,
        project_accounting_context,
        ussgl_transaction_code,
        ussgl_trx_code_context,
        project_id,
        task_id,
        expenditure_type,
        expenditure_item_date,
        pa_quantity,
        expenditure_organization_id,
        pa_default_dist_ccid,
        vendor_prepay_amount,
        payment_amount_total,
        awt_flag,
        awt_group_id,
        reference_1,
        reference_2,
        org_id,
        pre_withholding_amount,
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
        auto_tax_calc_flag,
        payment_cross_rate_type,
        payment_cross_rate_date,
        invoice_id,
        last_update_date,
        last_updated_by,
        vendor_id,
        invoice_num,
        set_of_books_id,
        invoice_currency_code,
        payment_currency_code,
		po_matched_flag,
		validation_worker_id,
		"pay_group_lookup_code#1",
		KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date
		)
    (SELECT
        pay_curr_invoice_amount,
        mrc_base_amount,
        mrc_exchange_rate,
        mrc_exchange_rate_type,
        mrc_exchange_date,
        gl_date,
        award_id,
        paid_on_behalf_employee_id,
        amt_due_ccard_company,
        amt_due_employee,
        approval_ready_flag,
        approval_iteration,
        wfapproval_status,
        requester_id,
        validation_request_id,
        validated_tax_amount,
        quick_credit,
        credited_invoice_id,
        distribution_set_id,
        application_id,
        product_table,
        reference_key1,
        reference_key2,
        reference_key3,
        reference_key4,
        reference_key5,
        total_tax_amount,
        self_assessed_tax_amount,
        tax_related_invoice_id,
        trx_business_category,
        user_defined_fisc_class,
        taxation_country,
        document_sub_type,
        supplier_tax_invoice_number,
        supplier_tax_invoice_date,
        supplier_tax_exchange_rate,
        tax_invoice_recording_date,
        tax_invoice_internal_seq,
        legal_entity_id,
        historical_flag,
        force_revalidation_flag,
        bank_charge_bearer,
        remittance_message1,
        remittance_message2,
        remittance_message3,
        unique_remittance_identifier,
        uri_check_digit,
        settlement_priority,
        payment_reason_code,
        payment_reason_comments,
        payment_method_code,
        delivery_channel_code,
        quick_po_header_id,
        net_of_retainage_flag,
        release_amount_net_of_tax,
        control_amount,
        party_id,
        party_site_id,
        pay_proc_trxn_type_code,
        payment_function,
        cust_registration_code,
        cust_registration_number,
        port_of_entry_code,
        external_bank_account_id,
        vendor_contact_id,
        internal_contact_email,
        disc_is_inv_less_tax_flag,
        exclude_freight_from_discount,
        pay_awt_group_id,
        original_invoice_amount,
        dispute_reason,
        remit_to_supplier_name,
        remit_to_supplier_id,
        remit_to_supplier_site,
        remit_to_supplier_site_id,
        relationship_id,
        payment_cross_rate,
        invoice_amount,
        vendor_site_id,
        amount_paid,
        discount_amount_taken,
        invoice_date,
        source,
        invoice_type_lookup_code,
        description,
        batch_id,
        amount_applicable_to_discount,
        tax_amount,
        terms_id,
        terms_date,
        payment_method_lookup_code,
        pay_group_lookup_code,
        accts_pay_code_combination_id,
        payment_status_flag,
        creation_date,
        created_by,
        base_amount,
        vat_code,
        last_update_login,
        exclusive_payment_flag,
        po_header_id,
        freight_amount,
        goods_received_date,
        invoice_received_date,
        voucher_num,
        approved_amount,
        recurring_payment_id,
        exchange_rate,
        exchange_rate_type,
        exchange_date,
        earliest_settlement_date,
        original_prepayment_amount,
        doc_sequence_id,
        doc_sequence_value,
        doc_category_code,
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
        attribute_category,
        approval_status,
        approval_description,
        invoice_distribution_total,
        posting_status,
        prepay_flag,
        authorized_by,
        cancelled_date,
        cancelled_by,
        cancelled_amount,
        temp_cancelled_amount,
        project_accounting_context,
        ussgl_transaction_code,
        ussgl_trx_code_context,
        project_id,
        task_id,
        expenditure_type,
        expenditure_item_date,
        pa_quantity,
        expenditure_organization_id,
        pa_default_dist_ccid,
        vendor_prepay_amount,
        payment_amount_total,
        awt_flag,
        awt_group_id,
        reference_1,
        reference_2,
        org_id,
        pre_withholding_amount,
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
        auto_tax_calc_flag,
        payment_cross_rate_type,
        payment_cross_rate_date,
        invoice_id,
        last_update_date,
        last_updated_by,
        vendor_id,
        invoice_num,
        set_of_books_id,
        invoice_currency_code,
        payment_currency_code,
		po_matched_flag,
		validation_worker_id,
		"pay_group_lookup_code#1",
        KCA_OPERATION,
       'N' as IS_DELETED_FLG,
	   cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	   kca_seq_date
    FROM
        bec_ods_stg.ap_invoices_all);

end;

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_invoices_all';
	
commit;