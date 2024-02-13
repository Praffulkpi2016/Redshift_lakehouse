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
	table bec_ods_stg.ap_payment_hist_dists;

insert
	into
	bec_ods_stg.ap_payment_hist_dists
(payment_hist_dist_id,
	accounting_event_id,
	pay_dist_lookup_code,
	invoice_distribution_id,
	amount,
	payment_history_id,
	invoice_payment_id,
	bank_curr_amount,
	cleared_base_amount,
	historical_flag,
	invoice_dist_amount,
	invoice_dist_base_amount,
	invoice_adjustment_event_id,
	matured_base_amount,
	paid_base_amount,
	rounding_amt,
	reversal_flag,
	reversed_pay_hist_dist_id,
	created_by,
	creation_date,
	last_update_date,
	last_updated_by,
	last_update_login,
	program_application_id,
	program_id,
	program_login_id,
	program_update_date,
	request_id,
	awt_related_id,
	release_inv_dist_derived_from,
	pa_addition_flag,
	amount_variance,
	invoice_base_amt_variance,
	quantity_variance,
	invoice_base_qty_variance,
	GAIN_LOSS_INDICATOR,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		payment_hist_dist_id,
		accounting_event_id,
		pay_dist_lookup_code,
		invoice_distribution_id,
		amount,
		payment_history_id,
		invoice_payment_id,
		bank_curr_amount,
		cleared_base_amount,
		historical_flag,
		invoice_dist_amount,
		invoice_dist_base_amount,
		invoice_adjustment_event_id,
		matured_base_amount,
		paid_base_amount,
		rounding_amt,
		reversal_flag,
		reversed_pay_hist_dist_id,
		created_by,
		creation_date,
		last_update_date,
		last_updated_by,
		last_update_login,
		program_application_id,
		program_id,
		program_login_id,
		program_update_date,
		request_id,
		awt_related_id,
		release_inv_dist_derived_from,
		pa_addition_flag,
		amount_variance,
		invoice_base_amt_variance,
		quantity_variance,
		invoice_base_qty_variance,
		GAIN_LOSS_INDICATOR,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.ap_payment_hist_dists
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (payment_hist_dist_id,KCA_SEQ_ID) in 
	(select payment_hist_dist_id,max(KCA_SEQ_ID) from bec_raw_dl_ext.ap_payment_hist_dists 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by payment_hist_dist_id)
     and kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'ap_payment_hist_dists')
			);
end;