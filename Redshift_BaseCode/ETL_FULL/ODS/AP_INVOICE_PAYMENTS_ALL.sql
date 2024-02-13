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

drop table if exists bec_ods.ap_invoice_payments_all;

CREATE TABLE IF NOT EXISTS bec_ods.ap_invoice_payments_all
(
	discount_lost NUMERIC(28,10)   ENCODE az64
	,discount_taken NUMERIC(28,10)   ENCODE az64
	,exchange_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,exchange_rate NUMERIC(28,10)   ENCODE az64
	,exchange_rate_type VARCHAR(30)   ENCODE lzo
	,gain_code_combination_id NUMERIC(15,0)   ENCODE az64
	,invoice_base_amount NUMERIC(28,10)   ENCODE az64
	,loss_code_combination_id NUMERIC(15,0)   ENCODE az64
	,payment_base_amount NUMERIC(28,10)   ENCODE az64
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
	,cash_je_batch_id NUMERIC(15,0)   ENCODE az64
	,future_pay_code_combination_id NUMERIC(15,0)   ENCODE az64
	,future_pay_posted_flag VARCHAR(1)   ENCODE lzo
	,je_batch_id NUMERIC(15,0)   ENCODE az64
	,electronic_transfer_id NUMERIC(15,0)   ENCODE az64
	,assets_addition_flag VARCHAR(1)   ENCODE lzo
	,invoice_payment_type VARCHAR(25)   ENCODE lzo
	,other_invoice_id NUMERIC(15,0)   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
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
	,external_bank_account_id NUMERIC(15,0)   ENCODE az64
	,accounting_event_id NUMERIC(15,0)   ENCODE az64
	,mrc_exchange_date VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_rate VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_rate_type VARCHAR(2000)   ENCODE lzo
	,mrc_gain_code_combination_id VARCHAR(2000)   ENCODE lzo
	,mrc_invoice_base_amount VARCHAR(2000)   ENCODE lzo
	,mrc_loss_code_combination_id VARCHAR(2000)   ENCODE lzo
	,mrc_payment_base_amount VARCHAR(2000)   ENCODE lzo
	,reversal_flag VARCHAR(1)   ENCODE lzo
	,reversal_inv_pmt_id NUMERIC(15,0)   ENCODE az64
	,iban_number VARCHAR(40)   ENCODE lzo
	,invoicing_party_id NUMERIC(15,0)   ENCODE az64
	,invoicing_party_site_id NUMERIC(15,0)   ENCODE az64
	,invoicing_vendor_site_id NUMERIC(15,0)   ENCODE az64
	,remit_to_supplier_name VARCHAR(240)   ENCODE lzo
	,remit_to_supplier_id NUMERIC(15,0)   ENCODE az64
	,remit_to_supplier_site VARCHAR(240)   ENCODE lzo
	,remit_to_supplier_site_id NUMERIC(15,0)   ENCODE az64
	,accounting_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,accrual_posted_flag VARCHAR(1)   ENCODE lzo
	,amount NUMERIC(28,10)   ENCODE az64
	,cash_posted_flag VARCHAR(1)   ENCODE lzo
	,check_id NUMERIC(15,0)   ENCODE az64
	,invoice_id NUMERIC(15,0)   ENCODE az64
	,invoice_payment_id NUMERIC(15,0)   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,payment_num NUMERIC(15,0)   ENCODE az64
	,period_name VARCHAR(15)   ENCODE lzo
	,posted_flag VARCHAR(1)   ENCODE lzo
	,set_of_books_id NUMERIC(15,0)   ENCODE az64
	,accts_pay_code_combination_id NUMERIC(15,0)   ENCODE az64
	,asset_code_combination_id NUMERIC(15,0)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,bank_account_num VARCHAR(30)   ENCODE lzo
	,bank_account_type VARCHAR(25)   ENCODE lzo
	,bank_num VARCHAR(25)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO
;
INSERT INTO bec_ods.ap_invoice_payments_all
        (discount_lost,
        discount_taken,
        exchange_date,
        exchange_rate,
        exchange_rate_type,
        gain_code_combination_id,
        invoice_base_amount,
        loss_code_combination_id,
        payment_base_amount,
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
        cash_je_batch_id,
        future_pay_code_combination_id,
        future_pay_posted_flag,
        je_batch_id,
        electronic_transfer_id,
        assets_addition_flag,
        invoice_payment_type,
        other_invoice_id,
        org_id,
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
        external_bank_account_id,
        accounting_event_id,
        mrc_exchange_date,
        mrc_exchange_rate,
        mrc_exchange_rate_type,
        mrc_gain_code_combination_id,
        mrc_invoice_base_amount,
        mrc_loss_code_combination_id,
        mrc_payment_base_amount,
        reversal_flag,
        reversal_inv_pmt_id,
        iban_number,
        invoicing_party_id,
        invoicing_party_site_id,
        invoicing_vendor_site_id,
        remit_to_supplier_name,
        remit_to_supplier_id,
        remit_to_supplier_site,
        remit_to_supplier_site_id,
        accounting_date,
        accrual_posted_flag,
        amount,
        cash_posted_flag,
        check_id,
        invoice_id,
        invoice_payment_id,
        last_updated_by,
        last_update_date,
        payment_num,
        period_name,
        posted_flag,
        set_of_books_id,
        accts_pay_code_combination_id,
        asset_code_combination_id,
        created_by,
        creation_date,
        last_update_login,
        bank_account_num,
        bank_account_type,
        bank_num,
		KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date
		)
    (SELECT
        discount_lost,
        discount_taken,
        exchange_date,
        exchange_rate,
        exchange_rate_type,
        gain_code_combination_id,
        invoice_base_amount,
        loss_code_combination_id,
        payment_base_amount,
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
        cash_je_batch_id,
        future_pay_code_combination_id,
        future_pay_posted_flag,
        je_batch_id,
        electronic_transfer_id,
        assets_addition_flag,
        invoice_payment_type,
        other_invoice_id,
        org_id,
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
        external_bank_account_id,
        accounting_event_id,
        mrc_exchange_date,
        mrc_exchange_rate,
        mrc_exchange_rate_type,
        mrc_gain_code_combination_id,
        mrc_invoice_base_amount,
        mrc_loss_code_combination_id,
        mrc_payment_base_amount,
        reversal_flag,
        reversal_inv_pmt_id,
        iban_number,
        invoicing_party_id,
        invoicing_party_site_id,
        invoicing_vendor_site_id,
        remit_to_supplier_name,
        remit_to_supplier_id,
        remit_to_supplier_site,
        remit_to_supplier_site_id,
        accounting_date,
        accrual_posted_flag,
        amount,
        cash_posted_flag,
        check_id,
        invoice_id,
        invoice_payment_id,
        last_updated_by,
        last_update_date,
        payment_num,
        period_name,
        posted_flag,
        set_of_books_id,
        accts_pay_code_combination_id,
        asset_code_combination_id,
        created_by,
        creation_date,
        last_update_login,
        bank_account_num,
        bank_account_type,
        bank_num,
        KCA_OPERATION,
       'N' as IS_DELETED_FLG,
	   cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	   kca_seq_date
    FROM
        bec_ods_stg.ap_invoice_payments_all);

end;

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_invoice_payments_all';
	
commit;