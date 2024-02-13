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

delete from bec_ods.AR_CASH_RECEIPT_HISTORY_ALL
where cash_receipt_history_id in (
select stg.cash_receipt_history_id from bec_ods.AR_CASH_RECEIPT_HISTORY_ALL ods, bec_ods_stg.AR_CASH_RECEIPT_HISTORY_ALL stg
where ods.cash_receipt_history_id = stg.cash_receipt_history_id and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.AR_CASH_RECEIPT_HISTORY_ALL
       (
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
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
(
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
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.AR_CASH_RECEIPT_HISTORY_ALL
	where kca_operation IN ('INSERT','UPDATE') 
	and (cash_receipt_history_id,kca_seq_id) in 
	(select cash_receipt_history_id,max(kca_seq_id) from bec_ods_stg.AR_CASH_RECEIPT_HISTORY_ALL 
     where kca_operation IN ('INSERT','UPDATE')
     group by cash_receipt_history_id)
);

commit;

-- Soft delete
update bec_ods.AR_CASH_RECEIPT_HISTORY_ALL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.AR_CASH_RECEIPT_HISTORY_ALL set IS_DELETED_FLG = 'Y'
where (cash_receipt_history_id )  in
(
select cash_receipt_history_id  from bec_raw_dl_ext.AR_CASH_RECEIPT_HISTORY_ALL
where (cash_receipt_history_id ,KCA_SEQ_ID)
in 
(
select cash_receipt_history_id ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.AR_CASH_RECEIPT_HISTORY_ALL
group by cash_receipt_history_id 
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'ar_cash_receipt_history_all';

commit;