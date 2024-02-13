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

delete from bec_ods.fa_deprn_detail
where (nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(DISTRIBUTION_ID,0),nvl(PERIOD_COUNTER,0)) in (
select nvl(stg.BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE, nvl(stg.ASSET_ID,0) as ASSET_ID, nvl(stg.DISTRIBUTION_ID,0) as DISTRIBUTION_ID, nvl(stg.PERIOD_COUNTER,0) as PERIOD_COUNTER
from bec_ods.fa_deprn_detail ods,  bec_ods_stg.fa_deprn_detail stg
where nvl(ods.BOOK_TYPE_CODE,'NA') = nvl(stg.BOOK_TYPE_CODE,'NA') 
and nvl(ods.ASSET_ID,0) = nvl(stg.ASSET_ID,0) 
and nvl(ods.DISTRIBUTION_ID,0) = nvl(stg.DISTRIBUTION_ID,0)
and nvl(ods.PERIOD_COUNTER,0) = nvl(stg.PERIOD_COUNTER,0)
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

INSERT INTO bec_ods.fa_deprn_detail
(
		BOOK_TYPE_CODE,
		ASSET_ID,
		PERIOD_COUNTER,
		DISTRIBUTION_ID,
		DEPRN_SOURCE_CODE,
		DEPRN_RUN_DATE,
		DEPRN_AMOUNT,
		YTD_DEPRN,
		DEPRN_RESERVE,
		ADDITION_COST_TO_CLEAR,
		COST,
		DEPRN_ADJUSTMENT_AMOUNT,
		DEPRN_EXPENSE_JE_LINE_NUM,
		DEPRN_RESERVE_JE_LINE_NUM,
		REVAL_AMORT_JE_LINE_NUM,
		REVAL_RESERVE_JE_LINE_NUM,
		JE_HEADER_ID,
		REVAL_AMORTIZATION,
		REVAL_DEPRN_EXPENSE,
		REVAL_RESERVE,
		YTD_REVAL_DEPRN_EXPENSE,
		BONUS_DEPRN_AMOUNT,
		BONUS_YTD_DEPRN,
		BONUS_DEPRN_RESERVE,
		BONUS_DEPRN_ADJUSTMENT_AMOUNT,
		BONUS_DEPRN_EXP_JE_LINE_NUM,
		BONUS_DEPRN_RSV_JE_LINE_NUM,
		DEPRN_EXPENSE_CCID,
		DEPRN_RESERVE_CCID,
		BONUS_DEPRN_EXPENSE_CCID,
		BONUS_DEPRN_RESERVE_CCID,
		REVAL_AMORT_CCID,
		REVAL_RESERVE_CCID,
		EVENT_ID,
		DEPRN_RUN_ID,
		IMPAIRMENT_AMOUNT,
		YTD_IMPAIRMENT,
		IMPAIRMENT_RESERVE,
		CAPITAL_ADJUSTMENT,
		GENERAL_FUND,
		REVAL_LOSS_BALANCE,
		KCA_OPERATION,
		IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date
)
(SELECT
		BOOK_TYPE_CODE,
		ASSET_ID,
		PERIOD_COUNTER,
		DISTRIBUTION_ID,
		DEPRN_SOURCE_CODE,
		DEPRN_RUN_DATE,
		DEPRN_AMOUNT,
		YTD_DEPRN,
		DEPRN_RESERVE,
		ADDITION_COST_TO_CLEAR,
		COST,
		DEPRN_ADJUSTMENT_AMOUNT,
		DEPRN_EXPENSE_JE_LINE_NUM,
		DEPRN_RESERVE_JE_LINE_NUM,
		REVAL_AMORT_JE_LINE_NUM,
		REVAL_RESERVE_JE_LINE_NUM,
		JE_HEADER_ID,
		REVAL_AMORTIZATION,
		REVAL_DEPRN_EXPENSE,
		REVAL_RESERVE,
		YTD_REVAL_DEPRN_EXPENSE,
		BONUS_DEPRN_AMOUNT,
		BONUS_YTD_DEPRN,
		BONUS_DEPRN_RESERVE,
		BONUS_DEPRN_ADJUSTMENT_AMOUNT,
		BONUS_DEPRN_EXP_JE_LINE_NUM,
		BONUS_DEPRN_RSV_JE_LINE_NUM,
		DEPRN_EXPENSE_CCID,
		DEPRN_RESERVE_CCID,
		BONUS_DEPRN_EXPENSE_CCID,
		BONUS_DEPRN_RESERVE_CCID,
		REVAL_AMORT_CCID,
		REVAL_RESERVE_CCID,
		EVENT_ID,
		DEPRN_RUN_ID,
		IMPAIRMENT_AMOUNT,
		YTD_IMPAIRMENT,
		IMPAIRMENT_RESERVE,
		CAPITAL_ADJUSTMENT,
		GENERAL_FUND,
		REVAL_LOSS_BALANCE,
		KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.fa_deprn_detail
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(DISTRIBUTION_ID,0),nvl(PERIOD_COUNTER,0),kca_seq_id) in 
	(select nvl(BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE,nvl(ASSET_ID,0) as ASSET_ID,nvl(DISTRIBUTION_ID,0) as DISTRIBUTION_ID,nvl(PERIOD_COUNTER,0) as PERIOD_COUNTER,max(kca_seq_id) from bec_ods_stg.fa_deprn_detail 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(DISTRIBUTION_ID,0),nvl(PERIOD_COUNTER,0))
);

commit;

-- Soft delete
update bec_ods.fa_deprn_detail set IS_DELETED_FLG = 'N';
commit;
update bec_ods.fa_deprn_detail set IS_DELETED_FLG = 'Y'
where (nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(DISTRIBUTION_ID,0),nvl(PERIOD_COUNTER,0))  in
(
select nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(DISTRIBUTION_ID,0),nvl(PERIOD_COUNTER,0) from bec_raw_dl_ext.fa_deprn_detail
where (nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(DISTRIBUTION_ID,0),nvl(PERIOD_COUNTER,0),KCA_SEQ_ID)
in 
(
select nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(DISTRIBUTION_ID,0),nvl(PERIOD_COUNTER,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.fa_deprn_detail
group by nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(DISTRIBUTION_ID,0),nvl(PERIOD_COUNTER,0)
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate(),load_type = 'I'
where ods_table_name = 'fa_deprn_detail';

commit;