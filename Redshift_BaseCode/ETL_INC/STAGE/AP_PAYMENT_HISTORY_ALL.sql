/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate
	table bec_ods_stg.ap_payment_history_all;

insert
	into
	bec_ods_stg.ap_payment_history_all
(payment_history_id,
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
	kca_seq_id,
	kca_seq_date)
(
	select
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
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.ap_payment_history_all
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (payment_history_id,KCA_SEQ_ID) in 
	(select payment_history_id,max(KCA_SEQ_ID) from bec_raw_dl_ext.ap_payment_history_all 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by payment_history_id)
     and kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ap_payment_history_all')
			);
end;