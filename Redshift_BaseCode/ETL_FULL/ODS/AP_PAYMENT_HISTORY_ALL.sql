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

DROP TABLE IF EXISTS bec_ods.ap_payment_history_all;

CREATE TABLE if NOT EXISTS bec_ods.ap_payment_history_all
(
	payment_history_id NUMERIC(15,0)   ENCODE az64
	,check_id NUMERIC(15,0)   ENCODE az64
	,accounting_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,transaction_type VARCHAR(30)   ENCODE lzo
	,posted_flag VARCHAR(1)   ENCODE lzo
	,matched_flag VARCHAR(1)   ENCODE lzo
	,accounting_event_id NUMERIC(15,0)   ENCODE az64
	,org_id NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,rev_pmt_hist_id NUMERIC(15,0)   ENCODE az64
	,trx_bank_amount NUMERIC(28,10)   ENCODE az64
	,errors_bank_amount NUMERIC(28,10)   ENCODE az64
	,charges_bank_amount NUMERIC(28,10)   ENCODE az64
	,trx_pmt_amount NUMERIC(28,10)   ENCODE az64
	,errors_pmt_amount NUMERIC(28,10)   ENCODE az64
	,charges_pmt_amount NUMERIC(28,10)   ENCODE az64
	,trx_base_amount NUMERIC(28,10)   ENCODE az64
	,errors_base_amount NUMERIC(28,10)   ENCODE az64
	,charges_base_amount NUMERIC(28,10)   ENCODE az64
	,bank_currency_code VARCHAR(15)   ENCODE lzo
	,bank_to_base_xrate_type VARCHAR(30)   ENCODE lzo
	,bank_to_base_xrate_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,bank_to_base_xrate NUMERIC(28,10)   ENCODE az64
	,pmt_currency_code VARCHAR(15)   ENCODE lzo
	,pmt_to_base_xrate_type VARCHAR(30)   ENCODE lzo
	,pmt_to_base_xrate_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,pmt_to_base_xrate NUMERIC(28,10)   ENCODE az64
	,mrc_pmt_to_base_xrate_type VARCHAR(2000)   ENCODE lzo
	,mrc_pmt_to_base_xrate_date VARCHAR(2000)   ENCODE lzo
	,mrc_pmt_to_base_xrate VARCHAR(2000)   ENCODE lzo
	,mrc_bank_to_base_xrate_type VARCHAR(2000)   ENCODE lzo
	,mrc_bank_to_base_xrate_date VARCHAR(2000)   ENCODE lzo
	,mrc_bank_to_base_xrate VARCHAR(2000)   ENCODE lzo
	,mrc_trx_base_amount VARCHAR(2000)   ENCODE lzo
	,mrc_errors_base_amount VARCHAR(2000)   ENCODE lzo
	,mrc_charges_base_amount VARCHAR(2000)   ENCODE lzo
	,related_event_id NUMERIC(15,0)   ENCODE az64
	,historical_flag VARCHAR(1)   ENCODE lzo
	,invoice_adjustment_event_id NUMERIC(15,0)   ENCODE az64
	,gain_loss_indicator VARCHAR(1)   ENCODE lzo
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.ap_payment_history_all (
    payment_history_id,
    check_id,
    accounting_date,
    transaction_type,
    posted_flag,
    matched_flag,
    accounting_event_id,
    org_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    program_update_date,
    program_application_id,
    program_id,
    request_id,
    rev_pmt_hist_id,
    trx_bank_amount,
    errors_bank_amount,
    charges_bank_amount,
    trx_pmt_amount,
    errors_pmt_amount,
    charges_pmt_amount,
    trx_base_amount,
    errors_base_amount,
    charges_base_amount,
    bank_currency_code,
    bank_to_base_xrate_type,
    bank_to_base_xrate_date,
    bank_to_base_xrate,
    pmt_currency_code,
    pmt_to_base_xrate_type,
    pmt_to_base_xrate_date,
    pmt_to_base_xrate,
    mrc_pmt_to_base_xrate_type,
    mrc_pmt_to_base_xrate_date,
    mrc_pmt_to_base_xrate,
    mrc_bank_to_base_xrate_type,
    mrc_bank_to_base_xrate_date,
    mrc_bank_to_base_xrate,
    mrc_trx_base_amount,
    mrc_errors_base_amount,
    mrc_charges_base_amount,
    related_event_id,
    historical_flag,
    invoice_adjustment_event_id,
    gain_loss_indicator,
	KCA_OPERATION,
    IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    (SELECT
        payment_history_id,
        check_id,
        accounting_date,
        transaction_type,
        posted_flag,
        matched_flag,
        accounting_event_id,
        org_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        program_update_date,
        program_application_id,
        program_id,
        request_id,
        rev_pmt_hist_id,
        trx_bank_amount,
        errors_bank_amount,
        charges_bank_amount,
        trx_pmt_amount,
        errors_pmt_amount,
        charges_pmt_amount,
        trx_base_amount,
        errors_base_amount,
        charges_base_amount,
        bank_currency_code,
        bank_to_base_xrate_type,
        bank_to_base_xrate_date,
        bank_to_base_xrate,
        pmt_currency_code,
        pmt_to_base_xrate_type,
        pmt_to_base_xrate_date,
        pmt_to_base_xrate,
        mrc_pmt_to_base_xrate_type,
        mrc_pmt_to_base_xrate_date,
        mrc_pmt_to_base_xrate,
        mrc_bank_to_base_xrate_type,
        mrc_bank_to_base_xrate_date,
        mrc_bank_to_base_xrate,
        mrc_trx_base_amount,
        mrc_errors_base_amount,
        mrc_charges_base_amount,
        related_event_id,
        historical_flag,
        invoice_adjustment_event_id,
        gain_loss_indicator,
        KCA_OPERATION,
       'N' as IS_DELETED_FLG,
	   cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	   kca_seq_date
    FROM
        bec_ods_stg.ap_payment_history_all);

end;

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ap_payment_history_all';
	
commit;
