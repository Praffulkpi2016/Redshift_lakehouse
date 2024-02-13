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

delete from bec_ods.ap_payment_hist_dists
where payment_hist_dist_id in (
select stg.payment_hist_dist_id 
from bec_ods.ap_payment_hist_dists ods, bec_ods_stg.ap_payment_hist_dists stg
where ods.payment_hist_dist_id = stg.payment_hist_dist_id
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert
	into
	bec_ods.ap_payment_hist_dists
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
	IS_DELETED_FLG,
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
		'N' AS IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.ap_payment_hist_dists
	where kca_operation IN ('INSERT','UPDATE') 
	and (payment_hist_dist_id,kca_seq_id) in 
	(select payment_hist_dist_id,max(kca_seq_id) from bec_ods_stg.ap_payment_hist_dists 
     where kca_operation IN ('INSERT','UPDATE')
     group by payment_hist_dist_id)	
);

commit;

-- Soft delete
update bec_ods.ap_payment_hist_dists set IS_DELETED_FLG = 'N';
commit;
update bec_ods.ap_payment_hist_dists set IS_DELETED_FLG = 'Y'
where (payment_hist_dist_id)  in
(
select payment_hist_dist_id from bec_raw_dl_ext.ap_payment_hist_dists
where (payment_hist_dist_id,KCA_SEQ_ID)
in 
(
select payment_hist_dist_id,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ap_payment_hist_dists
group by payment_hist_dist_id
) 
and kca_operation= 'DELETE'
);
commit;

end;

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'ap_payment_hist_dists';

commit;