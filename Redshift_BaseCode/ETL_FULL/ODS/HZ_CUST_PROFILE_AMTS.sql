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

DROP TABLE if exists bec_ods.hz_cust_profile_amts;

CREATE TABLE IF NOT EXISTS bec_ods.hz_cust_profile_amts
(
	cust_acct_profile_amt_id NUMERIC(15,0)   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cust_account_profile_id NUMERIC(15,0)   ENCODE az64
	,currency_code VARCHAR(15)   ENCODE lzo
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,trx_credit_limit NUMERIC(28,10)   ENCODE az64
	,overall_credit_limit NUMERIC(28,10)   ENCODE az64
	,min_dunning_amount NUMERIC(28,10)   ENCODE az64
	,min_dunning_invoice_amount NUMERIC(28,10)   ENCODE az64
	,max_interest_charge NUMERIC(28,10)   ENCODE az64
	,min_statement_amount NUMERIC(28,10)   ENCODE az64
	,auto_rec_min_receipt_amount NUMERIC(28,10)   ENCODE az64
	,interest_rate NUMERIC(28,10)   ENCODE az64
	,attribute_category VARCHAR(30)   ENCODE lzo
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
	,min_fc_balance_amount NUMERIC(28,10)   ENCODE az64
	,min_fc_invoice_amount NUMERIC(28,10)   ENCODE az64
	,cust_account_id NUMERIC(15,0)   ENCODE az64
	,site_use_id NUMERIC(15,0)   ENCODE az64
	,expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,wh_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,jgzz_attribute_category VARCHAR(30)   ENCODE lzo
	,jgzz_attribute1 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute2 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute3 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute4 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute5 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute6 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute7 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute8 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute9 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute10 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute11 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute12 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute13 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute14 VARCHAR(150)   ENCODE lzo
	,jgzz_attribute15 VARCHAR(150)   ENCODE lzo
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
	,global_attribute_category VARCHAR(30)   ENCODE lzo
	,object_version_number NUMERIC(15,0)   ENCODE az64
	,created_by_module VARCHAR(150)   ENCODE lzo
	,application_id NUMERIC(15,0)   ENCODE az64
	,exchange_rate_type VARCHAR(30)   ENCODE lzo
	,min_fc_invoice_overdue_type VARCHAR(30)   ENCODE lzo
	,min_fc_invoice_percent NUMERIC(28,10)   ENCODE az64
	,min_fc_balance_overdue_type VARCHAR(30)   ENCODE lzo
	,min_fc_balance_percent NUMERIC(28,10)   ENCODE az64
	,interest_type VARCHAR(30)   ENCODE lzo
	,interest_fixed_amount NUMERIC(28,10)   ENCODE az64
	,interest_schedule_id NUMERIC(15,0)   ENCODE az64
	,penalty_type VARCHAR(30)   ENCODE lzo
	,penalty_rate NUMERIC(28,10)   ENCODE az64
	,min_interest_charge NUMERIC(28,10)   ENCODE az64
	,penalty_fixed_amount NUMERIC(28,10)   ENCODE az64
	,penalty_schedule_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert
	into
	bec_ods.hz_cust_profile_amts
	(cust_acct_profile_amt_id,
	last_updated_by,
	last_update_date,
	created_by,
	creation_date,
	cust_account_profile_id,
	currency_code,
	last_update_login,
	trx_credit_limit,
	overall_credit_limit,
	min_dunning_amount,
	min_dunning_invoice_amount,
	max_interest_charge,
	min_statement_amount,
	auto_rec_min_receipt_amount,
	interest_rate,
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
	min_fc_balance_amount,
	min_fc_invoice_amount,
	cust_account_id,
	site_use_id,
	expiration_date,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	wh_update_date,
	jgzz_attribute_category,
	jgzz_attribute1,
	jgzz_attribute2,
	jgzz_attribute3,
	jgzz_attribute4,
	jgzz_attribute5,
	jgzz_attribute6,
	jgzz_attribute7,
	jgzz_attribute8,
	jgzz_attribute9,
	jgzz_attribute10,
	jgzz_attribute11,
	jgzz_attribute12,
	jgzz_attribute13,
	jgzz_attribute14,
	jgzz_attribute15,
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
	global_attribute_category,
	object_version_number,
	created_by_module,
	application_id,
	exchange_rate_type,
	min_fc_invoice_overdue_type,
	min_fc_invoice_percent,
	min_fc_balance_overdue_type,
	min_fc_balance_percent,
	interest_type,
	interest_fixed_amount,
	interest_schedule_id,
	penalty_type,
	penalty_rate,
	min_interest_charge,
	penalty_fixed_amount,
	penalty_schedule_id,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
	SELECT
	cust_acct_profile_amt_id,
	last_updated_by,
	last_update_date,
	created_by,
	creation_date,
	cust_account_profile_id,
	currency_code,
	last_update_login,
	trx_credit_limit,
	overall_credit_limit,
	min_dunning_amount,
	min_dunning_invoice_amount,
	max_interest_charge,
	min_statement_amount,
	auto_rec_min_receipt_amount,
	interest_rate,
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
	min_fc_balance_amount,
	min_fc_invoice_amount,
	cust_account_id,
	site_use_id,
	expiration_date,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	wh_update_date,
	jgzz_attribute_category,
	jgzz_attribute1,
	jgzz_attribute2,
	jgzz_attribute3,
	jgzz_attribute4,
	jgzz_attribute5,
	jgzz_attribute6,
	jgzz_attribute7,
	jgzz_attribute8,
	jgzz_attribute9,
	jgzz_attribute10,
	jgzz_attribute11,
	jgzz_attribute12,
	jgzz_attribute13,
	jgzz_attribute14,
	jgzz_attribute15,
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
	global_attribute_category,
	object_version_number,
	created_by_module,
	application_id,
	exchange_rate_type,
	min_fc_invoice_overdue_type,
	min_fc_invoice_percent,
	min_fc_balance_overdue_type,
	min_fc_balance_percent,
	interest_type,
	interest_fixed_amount,
	interest_schedule_id,
	penalty_type,
	penalty_rate,
	min_interest_charge,
	penalty_fixed_amount,
	penalty_schedule_id,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.HZ_CUST_PROFILE_AMTS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'hz_cust_profile_amts';
	
commit;
	
