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

truncate table bec_ods.XXBEC_SRVCREV_CDW_VARIABLE_STG; 
-- Insert records

insert into	bec_ods.XXBEC_SRVCREV_CDW_VARIABLE_STG
       ( "source",
	contract_group,
	contract_id,
	customer_trx_line_id,
	gl_dist_id,
	event_id,
	actual_forecast,
	ledger_id,
	ledger_name,
	org_id,
	org_name,
	site_id,
	revenue_type,
	transaction_currency,
	accounted_currency,
	period_name,
	revenue_amount_trans_curr,
	revenue_amount_acctd_curr,
	conversion_rate,
	tolling_rates_trans_curr,
	tolling_rates_acctd_curr,
	tmo_prcntge,
	last_update_date,
	extract_date,
	from_date,
	to_date,
	period_end_date,
	no_days,
	query_name,
	kwh,
	kwh_type,
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date)	
(
	select
		 "source",
	contract_group,
	contract_id,
	customer_trx_line_id,
	gl_dist_id,
	event_id,
	actual_forecast,
	ledger_id,
	ledger_name,
	org_id,
	org_name,
	site_id,
	revenue_type,
	transaction_currency,
	accounted_currency,
	period_name,
	revenue_amount_trans_curr,
	revenue_amount_acctd_curr,
	conversion_rate,
	tolling_rates_trans_curr,
	tolling_rates_acctd_curr,
	tmo_prcntge,
	last_update_date,
	extract_date,
	from_date,
	to_date,
	period_end_date,
	no_days,
	query_name,
	kwh,
	kwh_type,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.XXBEC_SRVCREV_CDW_VARIABLE_STG 
);

commit;

 
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'xxbec_srvcrev_cdw_variable_stg';

commit;