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
	table bec_ods_stg.FA_MC_DEPRN_SUMMARY;

insert
	into
	bec_ods_stg.FA_MC_DEPRN_SUMMARY
    (set_of_books_id,
	book_type_code,
	asset_id,
	deprn_run_date,
	deprn_amount,
	ytd_deprn,
	deprn_reserve,
	deprn_source_code,
	adjusted_cost,
	bonus_rate,
	ltd_production,
	period_counter,
	production,
	reval_amortization,
	reval_amortization_basis,
	reval_deprn_expense,
	reval_reserve,
	ytd_production,
	ytd_reval_deprn_expense,
	prior_fy_expense,
	converted_flag,
	bonus_deprn_amount,
	bonus_ytd_deprn,
	bonus_deprn_reserve,
	prior_fy_bonus_expense,
	deprn_override_flag,
	system_deprn_amount,
	system_bonus_deprn_amount,
	event_id,
	deprn_run_id,
	deprn_adjustment_amount,
	bonus_deprn_adjustment_amount,
	impairment_amount,
	ytd_impairment,
	impairment_reserve,
	capital_adjustment,
	general_fund,
	reval_loss_balance,
	unrevalued_cost,
	historical_nbv,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		set_of_books_id,
		book_type_code,
		asset_id,
		deprn_run_date,
		deprn_amount,
		ytd_deprn,
		deprn_reserve,
		deprn_source_code,
		adjusted_cost,
		bonus_rate,
		ltd_production,
		period_counter,
		production,
		reval_amortization,
		reval_amortization_basis,
		reval_deprn_expense,
		reval_reserve,
		ytd_production,
		ytd_reval_deprn_expense,
		prior_fy_expense,
		converted_flag,
		bonus_deprn_amount,
		bonus_ytd_deprn,
		bonus_deprn_reserve,
		prior_fy_bonus_expense,
		deprn_override_flag,
		system_deprn_amount,
		system_bonus_deprn_amount,
		event_id,
		deprn_run_id,
		deprn_adjustment_amount,
		bonus_deprn_adjustment_amount,
		impairment_amount,
		ytd_impairment,
		impairment_reserve,
		capital_adjustment,
		general_fund,
		reval_loss_balance,
		unrevalued_cost,
		historical_nbv,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.FA_MC_DEPRN_SUMMARY
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(BOOK_TYPE_CODE, 'NA'),
		nvl(ASSET_ID, 0),
		nvl(PERIOD_COUNTER, 0),
		nvl(SET_OF_BOOKS_ID, 0),
		KCA_SEQ_ID) in 
	(
		select
			nvl(BOOK_TYPE_CODE, 'NA') as BOOK_TYPE_CODE,
			nvl(ASSET_ID, 0) as ASSET_ID,
			nvl(PERIOD_COUNTER, 0) as PERIOD_COUNTER,
			nvl(SET_OF_BOOKS_ID, 0) as SET_OF_BOOKS_ID,
			max(kca_seq_id)
		from
			bec_raw_dl_ext.FA_MC_DEPRN_SUMMARY
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(BOOK_TYPE_CODE, 'NA'),
			nvl(ASSET_ID, 0),
			nvl(PERIOD_COUNTER, 0),
			nvl(SET_OF_BOOKS_ID, 0))
		and kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fa_mc_deprn_summary')
);
end;