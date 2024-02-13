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

truncate table bec_ods_stg.XXBEC_SRVCREV_CDW_VARIABLE_STG;

insert into	bec_ods_stg.XXBEC_SRVCREV_CDW_VARIABLE_STG
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
	kca_seq_id
	,KCA_SEQ_DATE)
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
	kca_operation,
	kca_seq_id
	,KCA_SEQ_DATE
	from bec_raw_dl_ext.XXBEC_SRVCREV_CDW_VARIABLE_STG 
);
end;