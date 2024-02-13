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

DROP TABLE if exists bec_ods.AR_CASH_RECEIPT_HISTORY_ALL;

CREATE TABLE IF NOT EXISTS bec_ods.ar_cash_receipt_history_all
(
	cash_receipt_history_id NUMERIC(15,0)   ENCODE az64
	,cash_receipt_id NUMERIC(15,0)   ENCODE az64
	,status VARCHAR(30)   ENCODE lzo
	,trx_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,amount NUMERIC(28,10)   ENCODE az64
	,first_posted_record_flag VARCHAR(1)   ENCODE lzo
	,postable_flag VARCHAR(1)   ENCODE lzo
	,factor_flag VARCHAR(1)   ENCODE lzo
	,gl_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,current_record_flag VARCHAR(1)   ENCODE lzo
	,batch_id NUMERIC(15,0)   ENCODE az64
	,account_code_combination_id NUMERIC(15,0)   ENCODE az64
	,reversal_gl_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,reversal_cash_receipt_hist_id NUMERIC(15,0)   ENCODE az64
	,factor_discount_amount NUMERIC(28,10)   ENCODE az64
	,bank_charge_account_ccid NUMERIC(15,0)   ENCODE az64
	,posting_control_id NUMERIC(15,0)   ENCODE az64
	,reversal_posting_control_id NUMERIC(15,0)   ENCODE az64
	,gl_posted_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,reversal_gl_posted_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,acctd_amount NUMERIC(28,10)   ENCODE az64
	,acctd_factor_discount_amount NUMERIC(28,10)   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,exchange_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,exchange_rate NUMERIC(28,10)   ENCODE az64
	,exchange_rate_type VARCHAR(30)   ENCODE lzo
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,prv_stat_cash_receipt_hist_id NUMERIC(15,0)   ENCODE az64
	,created_from VARCHAR(30)   ENCODE lzo
	,reversal_created_from VARCHAR(30)   ENCODE lzo
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
	,attribute_category VARCHAR(30)   ENCODE lzo
	,org_id NUMERIC(15,0)   ENCODE az64
	,MRC_POSTING_CONTROL_ID VARCHAR(2000)   ENCODE lzo
	,note_status VARCHAR(30)   ENCODE lzo 
	,mrc_gl_posted_date VARCHAR(2000)   ENCODE lzo
	,mrc_reversal_gl_posted_date VARCHAR(2000)   ENCODE lzo
	,mrc_acctd_amount VARCHAR(2000)   ENCODE lzo
	,mrc_acctd_factor_disc_amount VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_date VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_rate VARCHAR(2000)   ENCODE lzo
	,mrc_exchange_rate_type VARCHAR(2000)   ENCODE lzo 
	,event_id NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE AUTO;

INSERT INTO bec_ods.AR_CASH_RECEIPT_HISTORY_ALL (
    cash_receipt_history_id,
	cash_receipt_id,
	status,
	trx_date,
	amount,
	first_posted_record_flag,
	postable_flag,
	factor_flag,
	gl_date,
	current_record_flag,
	batch_id,
	account_code_combination_id,
	reversal_gl_date,
	reversal_cash_receipt_hist_id,
	factor_discount_amount,
	bank_charge_account_ccid,
	posting_control_id,
	reversal_posting_control_id,
	gl_posted_date,
	reversal_gl_posted_date,
	last_update_login,
	acctd_amount,
	acctd_factor_discount_amount,
	created_by,
	creation_date,
	exchange_date,
	exchange_rate,
	exchange_rate_type,
	last_update_date,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	last_updated_by,
	prv_stat_cash_receipt_hist_id,
	created_from,
	reversal_created_from,
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
	org_id,
	MRC_POSTING_CONTROL_ID,
	note_status, 
	mrc_gl_posted_date,
	mrc_reversal_gl_posted_date,
	mrc_acctd_amount,
	mrc_acctd_factor_disc_amount,
	mrc_exchange_date,
	mrc_exchange_rate,
	mrc_exchange_rate_type, 
	event_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        cash_receipt_history_id,
	cash_receipt_id,
	status,
	trx_date,
	amount,
	first_posted_record_flag,
	postable_flag,
	factor_flag,
	gl_date,
	current_record_flag,
	batch_id,
	account_code_combination_id,
	reversal_gl_date,
	reversal_cash_receipt_hist_id,
	factor_discount_amount,
	bank_charge_account_ccid,
	posting_control_id,
	reversal_posting_control_id,
	gl_posted_date,
	reversal_gl_posted_date,
	last_update_login,
	acctd_amount,
	acctd_factor_discount_amount,
	created_by,
	creation_date,
	exchange_date,
	exchange_rate,
	exchange_rate_type,
	last_update_date,
	program_application_id,
	program_id,
	program_update_date,
	request_id,
	last_updated_by,
	prv_stat_cash_receipt_hist_id,
	created_from,
	reversal_created_from,
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
	org_id,
	MRC_POSTING_CONTROL_ID,
	note_status, 
	mrc_gl_posted_date,
	mrc_reversal_gl_posted_date,
	mrc_acctd_amount,
	mrc_acctd_factor_disc_amount,
	mrc_exchange_date,
	mrc_exchange_rate,
	mrc_exchange_rate_type, 
	event_id,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.AR_CASH_RECEIPT_HISTORY_ALL;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'ar_cash_receipt_history_all';
	
commit;