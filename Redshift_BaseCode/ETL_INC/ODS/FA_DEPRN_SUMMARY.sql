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

delete from bec_ods.fa_deprn_summary
where (nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0)) in (
select nvl(stg.BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE, nvl(stg.ASSET_ID,0) as ASSET_ID, nvl(stg.PERIOD_COUNTER,0) as PERIOD_COUNTER
from bec_ods.fa_deprn_summary ods,  bec_ods_stg.fa_deprn_summary stg
where nvl(ods.BOOK_TYPE_CODE,'NA') = nvl(stg.BOOK_TYPE_CODE,'NA') 
and nvl(ods.ASSET_ID,0) = nvl(stg.ASSET_ID,0)
and nvl(ods.PERIOD_COUNTER,0) = nvl(stg.PERIOD_COUNTER,0)
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

INSERT INTO bec_ods.fa_deprn_summary
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
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
(SELECT
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
    'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.fa_deprn_summary
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0),kca_seq_id) in 
	(select nvl(BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE,nvl(ASSET_ID,0) as ASSET_ID,nvl(PERIOD_COUNTER,0) as PERIOD_COUNTER,max(kca_seq_id) from bec_ods_stg.fa_deprn_summary 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0))
);

commit;

-- Soft delete
update bec_ods.fa_deprn_summary set IS_DELETED_FLG = 'N';
commit;
update bec_ods.fa_deprn_summary set IS_DELETED_FLG = 'Y'
where (nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0))  in
(
select nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0) from bec_raw_dl_ext.fa_deprn_summary
where (nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0),KCA_SEQ_ID)
in 
(
select nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.fa_deprn_summary
group by nvl(BOOK_TYPE_CODE,'NA'),nvl(ASSET_ID,0),nvl(PERIOD_COUNTER,0)
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate(),load_type = 'I'
where ods_table_name = 'fa_deprn_summary';

commit;