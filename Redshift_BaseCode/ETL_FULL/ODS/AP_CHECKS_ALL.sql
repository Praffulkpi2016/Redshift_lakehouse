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

DROP TABLE IF EXISTS bec_ods.ap_checks_all;

CREATE TABLE if NOT EXISTS bec_ods.ap_checks_all
(
	amount NUMERIC(28,10)   ENCODE az64
	,bank_account_id NUMERIC(15,0)   ENCODE az64
	,bank_account_name VARCHAR(80)   ENCODE lzo
	,check_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,check_id NUMERIC(15,0)   ENCODE az64
	,check_number NUMERIC(15,0)   ENCODE az64
	,currency_code VARCHAR(15)   ENCODE lzo
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,payment_method_lookup_code VARCHAR(25)   ENCODE lzo
	,payment_type_flag VARCHAR(25)   ENCODE lzo
	,address_line1 VARCHAR(240)   ENCODE lzo
	,address_line2 VARCHAR(240)   ENCODE lzo
	,address_line3 VARCHAR(240)   ENCODE lzo
	,checkrun_name VARCHAR(255)   ENCODE lzo
	,check_format_id NUMERIC(15,0)   ENCODE az64
	,check_stock_id NUMERIC(15,0)   ENCODE az64
	,city VARCHAR(60)   ENCODE lzo
	,country VARCHAR(60)   ENCODE lzo
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,status_lookup_code VARCHAR(25)   ENCODE lzo
	,vendor_name VARCHAR(240)   ENCODE lzo
	,vendor_site_code VARCHAR(15)   ENCODE lzo
	,zip VARCHAR(60)   ENCODE lzo
	,bank_account_num VARCHAR(30)   ENCODE lzo
	,bank_account_type VARCHAR(25)   ENCODE lzo
	,bank_num VARCHAR(30)   ENCODE lzo
	,check_voucher_num NUMERIC(16,0)   ENCODE az64
	,cleared_amount NUMERIC(28,10)   ENCODE az64
	,cleared_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,doc_category_code VARCHAR(30)   ENCODE lzo
	,doc_sequence_id NUMERIC(15,0)   ENCODE az64
	,doc_sequence_value NUMERIC(15,0)   ENCODE az64
	,province VARCHAR(150)   ENCODE lzo
	,released_at VARCHAR(18)   ENCODE lzo
	,released_by NUMERIC(15,0)   ENCODE az64
	,state VARCHAR(150)   ENCODE lzo
	,stopped_at VARCHAR(18)   ENCODE lzo
	,stopped_by NUMERIC(15,0)   ENCODE az64
	,void_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,attribute_category VARCHAR(150)   ENCODE lzo
	,future_pay_due_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,treasury_pay_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,treasury_pay_number NUMERIC(15,0)   ENCODE az64
	,ussgl_transaction_code VARCHAR(30)   ENCODE lzo
	,ussgl_trx_code_context VARCHAR(30)   ENCODE lzo
	,withholding_status_lookup_code VARCHAR(25)   ENCODE lzo
	,reconciliation_batch_id NUMERIC(15,0)   ENCODE az64
	,cleared_base_amount NUMERIC(28,10)   ENCODE az64
	,cleared_exchange_rate NUMERIC(28,10)   ENCODE az64
	,cleared_exchange_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cleared_exchange_rate_type VARCHAR(30)   ENCODE lzo
	,address_line4 VARCHAR(240)   ENCODE lzo
	,county VARCHAR(150)   ENCODE lzo
	,address_style VARCHAR(30)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,vendor_site_id NUMERIC(15,0)   ENCODE az64
	,exchange_rate NUMERIC(28,10)   ENCODE az64
	,exchange_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,exchange_rate_type VARCHAR(30)   ENCODE lzo
	,base_amount NUMERIC(28,10)   ENCODE az64
	,checkrun_id NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,cleared_error_amount NUMERIC(28,10)   ENCODE az64
	,cleared_charges_amount NUMERIC(28,10)   ENCODE az64
	,cleared_error_base_amount NUMERIC(28,10)   ENCODE az64
	,cleared_charges_base_amount NUMERIC(28,10)   ENCODE az64
	,positive_pay_status_code VARCHAR(25)   ENCODE lzo
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
	,transfer_priority VARCHAR(25)   ENCODE lzo
	,external_bank_account_id NUMERIC(15,0)   ENCODE az64
	,stamp_duty_amt NUMERIC(28,10)   ENCODE az64
	,stamp_duty_base_amt NUMERIC(28,10)   ENCODE az64
	,mrc_cleared_base_amount VARCHAR(2000)   ENCODE lzo
	,mrc_cleared_exchange_rate VARCHAR(2000)   ENCODE lzo
	,mrc_cleared_exchange_date VARCHAR(2000)   ENCODE lzo
	,mrc_cleared_exchange_rate_type VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_rate VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_date VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_rate_type VARCHAR(2000)   ENCODE lzo
	,mrc_base_amount VARCHAR(2000)   ENCODE lzo
	,mrc_cleared_error_base_amount VARCHAR(2000)   ENCODE lzo
	,mrc_cleared_charges_base_amt VARCHAR(2000)   ENCODE lzo
	,mrc_stamp_duty_base_amt VARCHAR(2000)   ENCODE lzo
	,maturity_exchange_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,maturity_exchange_rate_type VARCHAR(30)   ENCODE lzo
	,maturity_exchange_rate NUMERIC(28,10)   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
	,actual_value_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,anticipated_value_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,released_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,stopped_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,mrc_maturity_exg_date VARCHAR(2000)   ENCODE lzo
	,mrc_maturity_exg_rate VARCHAR(2000)   ENCODE lzo
	,mrc_maturity_exg_rate_type VARCHAR(2000)   ENCODE lzo
	,iban_number VARCHAR(40)   ENCODE lzo
	,void_check_id NUMERIC(15,0)   ENCODE az64
	,void_check_number NUMERIC(15,0)   ENCODE az64
	,ce_bank_acct_use_id NUMERIC(15,0)   ENCODE az64
	,payment_method_code VARCHAR(30)   ENCODE lzo
	,party_id NUMERIC(15,0)   ENCODE az64
	,party_site_id NUMERIC(15,0)   ENCODE az64
	,payment_profile_id NUMERIC(15,0)   ENCODE az64
	,settlement_priority VARCHAR(30)   ENCODE lzo
	,bank_charge_bearer VARCHAR(30)   ENCODE lzo
	,legal_entity_id NUMERIC(15,0)   ENCODE az64
	,payment_document_id NUMERIC(15,0)   ENCODE az64
	,completed_pmts_group_id NUMERIC(15,0)   ENCODE az64
	,payment_id NUMERIC(15,0)   ENCODE az64
	,payment_instruction_id NUMERIC(15,0)   ENCODE az64
	,remit_to_supplier_name VARCHAR(240)   ENCODE lzo
	,remit_to_supplier_id NUMERIC(15,0)   ENCODE az64
	,remit_to_supplier_site VARCHAR(240)   ENCODE lzo
	,remit_to_supplier_site_id NUMERIC(15,0)   ENCODE az64
	,relationship_id NUMERIC(15,0)   ENCODE az64
	,paycard_authorization_number VARCHAR(30)   ENCODE lzo
	,paycard_reference_id NUMERIC(15,0)   ENCODE az64
    ,ACKNOWLEDGED_FLAG VARCHAR(1) ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.ap_checks_all (
    amount,
    bank_account_id,
    bank_account_name,
    check_date,
    check_id,
    check_number,
    currency_code,
    last_updated_by,
    last_update_date,
    payment_method_lookup_code,
    payment_type_flag,
    address_line1,
    address_line2,
    address_line3,
    checkrun_name,
    check_format_id,
    check_stock_id,
    city,
    country,
    created_by,
    creation_date,
    last_update_login,
    status_lookup_code,
    vendor_name,
    vendor_site_code,
    zip,
    bank_account_num,
    bank_account_type,
    bank_num,
    check_voucher_num,
    cleared_amount,
    cleared_date,
    doc_category_code,
    doc_sequence_id,
    doc_sequence_value,
    province,
    released_at,
    released_by,
    state,
    stopped_at,
    stopped_by,
    void_date,
    attribute1,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute_category,
    future_pay_due_date,
    treasury_pay_date,
    treasury_pay_number,
    ussgl_transaction_code,
    ussgl_trx_code_context,
    withholding_status_lookup_code,
    reconciliation_batch_id,
    cleared_base_amount,
    cleared_exchange_rate,
    cleared_exchange_date,
    cleared_exchange_rate_type,
    address_line4,
    county,
    address_style,
    org_id,
    vendor_id,
    vendor_site_id,
    exchange_rate,
    exchange_date,
    exchange_rate_type,
    base_amount,
    checkrun_id,
    request_id,
    cleared_error_amount,
    cleared_charges_amount,
    cleared_error_base_amount,
    cleared_charges_base_amount,
    positive_pay_status_code,
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
    transfer_priority,
    external_bank_account_id,
    stamp_duty_amt,
    stamp_duty_base_amt,
    mrc_cleared_base_amount,
    mrc_cleared_exchange_rate,
    mrc_cleared_exchange_date,
    mrc_cleared_exchange_rate_type,
    mrc_exchange_rate,
    mrc_exchange_date,
    mrc_exchange_rate_type,
    mrc_base_amount,
    mrc_cleared_error_base_amount,
    mrc_cleared_charges_base_amt,
    mrc_stamp_duty_base_amt,
    maturity_exchange_date,
    maturity_exchange_rate_type,
    maturity_exchange_rate,
    description,
    actual_value_date,
    anticipated_value_date,
    released_date,
    stopped_date,
    mrc_maturity_exg_date,
    mrc_maturity_exg_rate,
    mrc_maturity_exg_rate_type,
    iban_number,
    void_check_id,
    void_check_number,
    ce_bank_acct_use_id,
    payment_method_code,
    party_id,
    party_site_id,
    payment_profile_id,
    settlement_priority,
    bank_charge_bearer,
    legal_entity_id,
    payment_document_id,
    completed_pmts_group_id,
    payment_id,
    payment_instruction_id,
    remit_to_supplier_name,
    remit_to_supplier_id,
    remit_to_supplier_site,
    remit_to_supplier_site_id,
    relationship_id,
    paycard_authorization_number,
    paycard_reference_id,
	ACKNOWLEDGED_FLAG,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date

)
    SELECT
        amount,
        bank_account_id,
        bank_account_name,
        check_date,
        check_id,
        check_number,
        currency_code,
        last_updated_by,
        last_update_date,
        payment_method_lookup_code,
        payment_type_flag,
        address_line1,
        address_line2,
        address_line3,
        checkrun_name,
        check_format_id,
        check_stock_id,
        city,
        country,
        created_by,
        creation_date,
        last_update_login,
        status_lookup_code,
        vendor_name,
        vendor_site_code,
        zip,
        bank_account_num,
        bank_account_type,
        bank_num,
        check_voucher_num,
        cleared_amount,
        cleared_date,
        doc_category_code,
        doc_sequence_id,
        doc_sequence_value,
        province,
        released_at,
        released_by,
        state,
        stopped_at,
        stopped_by,
        void_date,
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute_category,
        future_pay_due_date,
        treasury_pay_date,
        treasury_pay_number,
        ussgl_transaction_code,
        ussgl_trx_code_context,
        withholding_status_lookup_code,
        reconciliation_batch_id,
        cleared_base_amount,
        cleared_exchange_rate,
        cleared_exchange_date,
        cleared_exchange_rate_type,
        address_line4,
        county,
        address_style,
        org_id,
        vendor_id,
        vendor_site_id,
        exchange_rate,
        exchange_date,
        exchange_rate_type,
        base_amount,
        checkrun_id,
        request_id,
        cleared_error_amount,
        cleared_charges_amount,
        cleared_error_base_amount,
        cleared_charges_base_amount,
        positive_pay_status_code,
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
        transfer_priority,
        external_bank_account_id,
        stamp_duty_amt,
        stamp_duty_base_amt,
        mrc_cleared_base_amount,
        mrc_cleared_exchange_rate,
        mrc_cleared_exchange_date,
        mrc_cleared_exchange_rate_type,
        mrc_exchange_rate,
        mrc_exchange_date,
        mrc_exchange_rate_type,
        mrc_base_amount,
        mrc_cleared_error_base_amount,
        mrc_cleared_charges_base_amt,
        mrc_stamp_duty_base_amt,
        maturity_exchange_date,
        maturity_exchange_rate_type,
        maturity_exchange_rate,
        description,
        actual_value_date,
        anticipated_value_date,
        released_date,
        stopped_date,
        mrc_maturity_exg_date,
        mrc_maturity_exg_rate,
        mrc_maturity_exg_rate_type,
        iban_number,
        void_check_id,
        void_check_number,
        ce_bank_acct_use_id,
        payment_method_code,
        party_id,
        party_site_id,
        payment_profile_id,
        settlement_priority,
        bank_charge_bearer,
        legal_entity_id,
        payment_document_id,
        completed_pmts_group_id,
        payment_id,
        payment_instruction_id,
        remit_to_supplier_name,
        remit_to_supplier_id,
        remit_to_supplier_site,
        remit_to_supplier_site_id,
        relationship_id,
        paycard_authorization_number,
        paycard_reference_id,
		ACKNOWLEDGED_FLAG,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date

    FROM
        bec_ods_stg.ap_checks_all;
	
end;	

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_checks_all';
	
commit;