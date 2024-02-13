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
	table bec_ods_stg.FA_MC_DEPRN_DETAIL;

insert
	into
	bec_ods_stg.FA_MC_DEPRN_DETAIL
    (set_of_books_id,
	book_type_code,
	asset_id,
	period_counter,
	distribution_id,
	deprn_source_code,
	deprn_run_date,
	deprn_amount,
	ytd_deprn,
	deprn_reserve,
	addition_cost_to_clear,
	cost,
	deprn_adjustment_amount,
	deprn_expense_je_line_num,
	deprn_reserve_je_line_num,
	reval_amort_je_line_num,
	reval_reserve_je_line_num,
	je_header_id,
	reval_amortization,
	reval_deprn_expense,
	reval_reserve,
	ytd_reval_deprn_expense,
	source_deprn_amount,
	source_ytd_deprn,
	source_deprn_reserve,
	source_addition_cost_to_clear,
	source_deprn_adjustment_amount,
	source_reval_amortization,
	source_reval_deprn_expense,
	source_reval_reserve,
	source_ytd_reval_deprn_expense,
	converted_flag,
	bonus_deprn_amount,
	bonus_ytd_deprn,
	bonus_deprn_reserve,
	bonus_deprn_adjustment_amount,
	bonus_deprn_exp_je_line_num,
	bonus_deprn_rsv_je_line_num,
	deprn_expense_ccid,
	deprn_reserve_ccid,
	bonus_deprn_expense_ccid,
	bonus_deprn_reserve_ccid,
	reval_amort_ccid,
	reval_reserve_ccid,
	event_id,
	deprn_run_id,
	impairment_amount,
	ytd_impairment,
	impairment_reserve,
	capital_adjustment,
	general_fund,
	reval_loss_balance, 
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		set_of_books_id,
		book_type_code,
		asset_id,
		period_counter,
		distribution_id,
		deprn_source_code,
		deprn_run_date,
		deprn_amount,
		ytd_deprn,
		deprn_reserve,
		addition_cost_to_clear,
		cost,
		deprn_adjustment_amount,
		deprn_expense_je_line_num,
		deprn_reserve_je_line_num,
		reval_amort_je_line_num,
		reval_reserve_je_line_num,
		je_header_id,
		reval_amortization,
		reval_deprn_expense,
		reval_reserve,
		ytd_reval_deprn_expense,
		source_deprn_amount,
		source_ytd_deprn,
		source_deprn_reserve,
		source_addition_cost_to_clear,
		source_deprn_adjustment_amount,
		source_reval_amortization,
		source_reval_deprn_expense,
		source_reval_reserve,
		source_ytd_reval_deprn_expense,
		converted_flag,
		bonus_deprn_amount,
		bonus_ytd_deprn,
		bonus_deprn_reserve,
		bonus_deprn_adjustment_amount,
		bonus_deprn_exp_je_line_num,
		bonus_deprn_rsv_je_line_num,
		deprn_expense_ccid,
		deprn_reserve_ccid,
		bonus_deprn_expense_ccid,
		bonus_deprn_reserve_ccid,
		reval_amort_ccid,
		reval_reserve_ccid,
		event_id,
		deprn_run_id,
		impairment_amount,
		ytd_impairment,
		impairment_reserve,
		capital_adjustment,
		general_fund,
		reval_loss_balance, 
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.FA_MC_DEPRN_DETAIL
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(BOOK_TYPE_CODE, 'NA'),
		nvl(ASSET_ID, 0),
		nvl(PERIOD_COUNTER, 0),
		nvl(DISTRIBUTION_ID, 0),
		nvl(SET_OF_BOOKS_ID, 0),
		KCA_SEQ_ID) in 
	(
		select
			nvl(BOOK_TYPE_CODE, 'NA') as BOOK_TYPE_CODE,
			nvl(ASSET_ID, 0) as ASSET_ID,
			nvl(PERIOD_COUNTER, 0) as PERIOD_COUNTER,
			nvl(DISTRIBUTION_ID, 0) as DISTRIBUTION_ID,
			nvl(SET_OF_BOOKS_ID, 0) as SET_OF_BOOKS_ID,
			max(kca_seq_id)
		from
			bec_raw_dl_ext.FA_MC_DEPRN_DETAIL
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(BOOK_TYPE_CODE, 'NA'),
			nvl(ASSET_ID, 0),
			nvl(PERIOD_COUNTER, 0),
			nvl(DISTRIBUTION_ID, 0),
			nvl(SET_OF_BOOKS_ID, 0))
			and kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fa_mc_deprn_detail')
);
end;