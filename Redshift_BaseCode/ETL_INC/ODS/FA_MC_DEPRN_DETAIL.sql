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

delete
from
	bec_ods.FA_MC_DEPRN_DETAIL
where
	(
	nvl(BOOK_TYPE_CODE, 'NA'),
	nvl(ASSET_ID,0),
	nvl(PERIOD_COUNTER,0),
	nvl(DISTRIBUTION_ID,0),
	nvl(SET_OF_BOOKS_ID,0)
	) in 
	(
	select
		nvl(stg.BOOK_TYPE_CODE, 'NA') as BOOK_TYPE_CODE,
		nvl(stg.ASSET_ID,0) as ASSET_ID,
		nvl(stg.PERIOD_COUNTER,0) as PERIOD_COUNTER,
		nvl(stg.DISTRIBUTION_ID,0) as DISTRIBUTION_ID,
		nvl(stg.SET_OF_BOOKS_ID,0) as SET_OF_BOOKS_ID
	from
		bec_ods.FA_MC_DEPRN_DETAIL ods,
		bec_ods_stg.FA_MC_DEPRN_DETAIL stg
	where
		NVL(ods.BOOK_TYPE_CODE, 'NA') = NVL(stg.BOOK_TYPE_CODE, 'NA')
	and NVL(ods.ASSET_ID,0) = NVL(stg.ASSET_ID,0)
	and NVL(ods.PERIOD_COUNTER,0) = NVL(stg.PERIOD_COUNTER,0)
	and NVL(ods.DISTRIBUTION_ID,0) = NVL(stg.DISTRIBUTION_ID,0)
	and NVL(ods.SET_OF_BOOKS_ID,0) = NVL(stg.SET_OF_BOOKS_ID,0)
	and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert into	bec_ods.FA_MC_DEPRN_DETAIL
( set_of_books_id,
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
	kca_operation,
	IS_DELETED_FLG,
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
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.FA_MC_DEPRN_DETAIL
	where
		kca_operation IN ('INSERT','UPDATE')
		and (nvl(BOOK_TYPE_CODE, 'NA'),
		nvl(ASSET_ID,0),
		nvl(PERIOD_COUNTER,0),
		nvl(DISTRIBUTION_ID,0),
		nvl(SET_OF_BOOKS_ID,0),
		KCA_SEQ_ID) in 
	(
		select
			nvl(BOOK_TYPE_CODE, 'NA') as BOOK_TYPE_CODE,
			nvl(ASSET_ID,0) as ASSET_ID,
			nvl(PERIOD_COUNTER,0) as PERIOD_COUNTER,
			nvl(DISTRIBUTION_ID,0) as DISTRIBUTION_ID,
			nvl(SET_OF_BOOKS_ID,0) as SET_OF_BOOKS_ID,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.FA_MC_DEPRN_DETAIL
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			nvl(BOOK_TYPE_CODE, 'NA'),
			nvl(ASSET_ID,0),
			nvl(PERIOD_COUNTER,0),
			nvl(DISTRIBUTION_ID,0),
			nvl(SET_OF_BOOKS_ID,0) 
			)	
	);

commit;

-- Soft delete
update bec_ods.FA_MC_DEPRN_DETAIL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FA_MC_DEPRN_DETAIL set IS_DELETED_FLG = 'Y'
where (nvl(BOOK_TYPE_CODE, 'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0),nvl(DISTRIBUTION_ID,0),nvl(SET_OF_BOOKS_ID,0))  in
(
select nvl(BOOK_TYPE_CODE, 'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0),nvl(DISTRIBUTION_ID,0),nvl(SET_OF_BOOKS_ID,0) from bec_raw_dl_ext.FA_MC_DEPRN_DETAIL
where (nvl(BOOK_TYPE_CODE, 'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0),nvl(DISTRIBUTION_ID,0),nvl(SET_OF_BOOKS_ID,0),KCA_SEQ_ID)
in 
(
select nvl(BOOK_TYPE_CODE, 'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0),nvl(DISTRIBUTION_ID,0),nvl(SET_OF_BOOKS_ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FA_MC_DEPRN_DETAIL
group by nvl(BOOK_TYPE_CODE, 'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0),nvl(DISTRIBUTION_ID,0),nvl(SET_OF_BOOKS_ID,0)
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
	ods_table_name = 'fa_mc_deprn_detail';

commit;