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
begin;

-- Delete Records

delete from bec_ods.HZ_CUST_PROFILE_AMTS
where (CUST_ACCT_PROFILE_AMT_ID) in (
select stg.CUST_ACCT_PROFILE_AMT_ID from bec_ods.HZ_CUST_PROFILE_AMTS ods, bec_ods_stg.HZ_CUST_PROFILE_AMTS stg
where ods.CUST_ACCT_PROFILE_AMT_ID = stg.CUST_ACCT_PROFILE_AMT_ID  and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

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
        bec_ods_stg.HZ_CUST_PROFILE_AMTS
	where kca_operation IN ('INSERT','UPDATE') 
	and (CUST_ACCT_PROFILE_AMT_ID,kca_seq_id) in 
	(select CUST_ACCT_PROFILE_AMT_ID,max(kca_seq_id) from bec_ods_stg.HZ_CUST_PROFILE_AMTS 
     where kca_operation IN ('INSERT','UPDATE')
     group by CUST_ACCT_PROFILE_AMT_ID);

commit;
 
-- Soft delete
update bec_ods.HZ_CUST_PROFILE_AMTS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.HZ_CUST_PROFILE_AMTS set IS_DELETED_FLG = 'Y'
where (CUST_ACCT_PROFILE_AMT_ID)  in
(
select CUST_ACCT_PROFILE_AMT_ID from bec_raw_dl_ext.HZ_CUST_PROFILE_AMTS
where (CUST_ACCT_PROFILE_AMT_ID,KCA_SEQ_ID)
in 
(
select CUST_ACCT_PROFILE_AMT_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.HZ_CUST_PROFILE_AMTS
group by CUST_ACCT_PROFILE_AMT_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'hz_cust_profile_amts';

commit;