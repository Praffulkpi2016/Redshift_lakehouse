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

truncate table bec_ods_stg.fa_deprn_summary;

insert into	bec_ods_stg.fa_deprn_summary
   (
	BOOK_TYPE_CODE,
	ASSET_ID,
	DEPRN_RUN_DATE,
	DEPRN_AMOUNT,
	YTD_DEPRN,
	DEPRN_RESERVE,
	DEPRN_SOURCE_CODE,
	ADJUSTED_COST,
	BONUS_RATE,
	LTD_PRODUCTION,
	PERIOD_COUNTER,
	PRODUCTION,
	REVAL_AMORTIZATION,
	REVAL_AMORTIZATION_BASIS,
	REVAL_DEPRN_EXPENSE,
	REVAL_RESERVE,
	YTD_PRODUCTION,
	YTD_REVAL_DEPRN_EXPENSE,
	PRIOR_FY_EXPENSE,
	BONUS_DEPRN_AMOUNT,
	BONUS_YTD_DEPRN,
	BONUS_DEPRN_RESERVE,
	PRIOR_FY_BONUS_EXPENSE,
	DEPRN_OVERRIDE_FLAG,
	SYSTEM_DEPRN_AMOUNT,
	SYSTEM_BONUS_DEPRN_AMOUNT,
	EVENT_ID,
	DEPRN_RUN_ID,
	DEPRN_ADJUSTMENT_AMOUNT,
	BONUS_DEPRN_ADJUSTMENT_AMOUNT,
	IMPAIRMENT_AMOUNT,
	YTD_IMPAIRMENT,
	IMPAIRMENT_RESERVE,
	CAPITAL_ADJUSTMENT,
	GENERAL_FUND,
	REVAL_LOSS_BALANCE,
	UNREVALUED_COST,
	HISTORICAL_NBV,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
	)
(
	select
	BOOK_TYPE_CODE,
	ASSET_ID,
	DEPRN_RUN_DATE,
	DEPRN_AMOUNT,
	YTD_DEPRN,
	DEPRN_RESERVE,
	DEPRN_SOURCE_CODE,
	ADJUSTED_COST,
	BONUS_RATE,
	LTD_PRODUCTION,
	PERIOD_COUNTER,
	PRODUCTION,
	REVAL_AMORTIZATION,
	REVAL_AMORTIZATION_BASIS,
	REVAL_DEPRN_EXPENSE,
	REVAL_RESERVE,
	YTD_PRODUCTION,
	YTD_REVAL_DEPRN_EXPENSE,
	PRIOR_FY_EXPENSE,
	BONUS_DEPRN_AMOUNT,
	BONUS_YTD_DEPRN,
	BONUS_DEPRN_RESERVE,
	PRIOR_FY_BONUS_EXPENSE,
	DEPRN_OVERRIDE_FLAG,
	SYSTEM_DEPRN_AMOUNT,
	SYSTEM_BONUS_DEPRN_AMOUNT,
	EVENT_ID,
	DEPRN_RUN_ID,
	DEPRN_ADJUSTMENT_AMOUNT,
	BONUS_DEPRN_ADJUSTMENT_AMOUNT,
	IMPAIRMENT_AMOUNT,
	YTD_IMPAIRMENT,
	IMPAIRMENT_RESERVE,
	CAPITAL_ADJUSTMENT,
	GENERAL_FUND,
	REVAL_LOSS_BALANCE,
	UNREVALUED_COST,
	HISTORICAL_NBV,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.fa_deprn_summary
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0),kca_seq_id) in 
	(select nvl(BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE,nvl(ASSET_ID,0) as ASSET_ID,nvl(PERIOD_COUNTER,0) as PERIOD_COUNTER,max(kca_seq_id) 
from bec_raw_dl_ext.fa_deprn_summary 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0))
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fa_deprn_summary')

);
end;


