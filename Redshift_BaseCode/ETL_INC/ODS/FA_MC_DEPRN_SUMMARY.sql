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
	bec_ods.FA_MC_DEPRN_SUMMARY
where
	(
	nvl(BOOK_TYPE_CODE, 'NA'),
	nvl(ASSET_ID,0),
	nvl(PERIOD_COUNTER,0),
	nvl(SET_OF_BOOKS_ID,0)
	) in 
	(
	select
		   nvl(stg.BOOK_TYPE_CODE, 'NA') as BOOK_TYPE_CODE,
		nvl(stg.ASSET_ID,0) as ASSET_ID,
		nvl(stg.PERIOD_COUNTER,0) as PERIOD_COUNTER,
		nvl(stg.SET_OF_BOOKS_ID,0) as SET_OF_BOOKS_ID
	from
		bec_ods.FA_MC_DEPRN_SUMMARY ods,
		bec_ods_stg.FA_MC_DEPRN_SUMMARY stg
	where
		NVL(ods.BOOK_TYPE_CODE, 'NA') = NVL(stg.BOOK_TYPE_CODE, 'NA')
		and NVL(ods.ASSET_ID,0) = NVL(stg.ASSET_ID,0)
		and NVL(ods.PERIOD_COUNTER,0) = NVL(stg.PERIOD_COUNTER,0)
		and NVL(ods.SET_OF_BOOKS_ID,0) = NVL(stg.SET_OF_BOOKS_ID,0)
			and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.FA_MC_DEPRN_SUMMARY (
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
	kca_operation,
	IS_DELETED_FLG,
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
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.FA_MC_DEPRN_SUMMARY
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		nvl(BOOK_TYPE_CODE, 'NA'),
		nvl(ASSET_ID,0),
		nvl(PERIOD_COUNTER,0),
		nvl(SET_OF_BOOKS_ID,0),
		KCA_SEQ_ID
		) in 
	(
		select
			nvl(BOOK_TYPE_CODE, 'NA') as BOOK_TYPE_CODE,
			nvl(ASSET_ID,0) as ASSET_ID,
			nvl(PERIOD_COUNTER,0) as PERIOD_COUNTER,
			nvl(SET_OF_BOOKS_ID,0) as SET_OF_BOOKS_ID,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.FA_MC_DEPRN_SUMMARY
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			nvl(BOOK_TYPE_CODE, 'NA'),
			nvl(ASSET_ID,0),
			nvl(PERIOD_COUNTER,0),
			nvl(SET_OF_BOOKS_ID,0) 
			)	
	);

commit;

-- Soft delete
update bec_ods.FA_MC_DEPRN_SUMMARY set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FA_MC_DEPRN_SUMMARY set IS_DELETED_FLG = 'Y'
where (nvl(BOOK_TYPE_CODE, 'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0))  in
(
select nvl(BOOK_TYPE_CODE, 'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0) from bec_raw_dl_ext.FA_MC_DEPRN_SUMMARY
where (nvl(BOOK_TYPE_CODE, 'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0),KCA_SEQ_ID)
in 
(
select nvl(BOOK_TYPE_CODE, 'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FA_MC_DEPRN_SUMMARY
group by nvl(BOOK_TYPE_CODE, 'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0)
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
	ods_table_name = 'fa_mc_deprn_summary';

commit;