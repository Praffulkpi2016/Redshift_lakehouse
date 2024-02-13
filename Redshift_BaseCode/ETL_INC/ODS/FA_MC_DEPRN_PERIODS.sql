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

delete from bec_ods.FA_MC_DEPRN_PERIODS
where (nvl(BOOK_TYPE_CODE,'NA'),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0)) in (
select nvl(stg.BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE,nvl(stg.PERIOD_COUNTER,0) as PERIOD_COUNTER,nvl(stg.SET_OF_BOOKS_ID,0) as SET_OF_BOOKS_ID from bec_ods.FA_MC_DEPRN_PERIODS ods, bec_ods_stg.FA_MC_DEPRN_PERIODS stg
where nvl(ods.BOOK_TYPE_CODE,'NA') = nvl(stg.BOOK_TYPE_CODE,'NA') and nvl(ods.PERIOD_COUNTER,0) = nvl(stg.PERIOD_COUNTER,0) and nvl(ods.SET_OF_BOOKS_ID,0) = nvl(stg.SET_OF_BOOKS_ID,0) and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.FA_MC_DEPRN_PERIODS
       (	
		SET_OF_BOOKS_ID,
		BOOK_TYPE_CODE, 
		PERIOD_NAME,
		PERIOD_COUNTER,
		FISCAL_YEAR,
		PERIOD_NUM,
		PERIOD_OPEN_DATE,
		PERIOD_CLOSE_DATE,
		DEPRECIATION_BATCH_ID,
		RETIREMENT_BATCH_ID,
		RECLASS_BATCH_ID,
		TRANSFER_BATCH_ID,
		ADDITION_BATCH_ID,
		ADJUSTMENT_BATCH_ID,
		DEFERRED_DEPRN_BATCH_ID,
		CALENDAR_PERIOD_OPEN_DATE,
		CALENDAR_PERIOD_CLOSE_DATE,
		CIP_ADDITION_BATCH_ID,
		CIP_ADJUSTMENT_BATCH_ID,
		CIP_RECLASS_BATCH_ID,
		CIP_RETIREMENT_BATCH_ID,
		CIP_REVAL_BATCH_ID,
		CIP_TRANSFER_BATCH_ID,
		REVAL_BATCH_ID,
		DEPRN_ADJUSTMENT_BATCH_ID,
		DEPRN_RUN,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		SET_OF_BOOKS_ID,
		BOOK_TYPE_CODE, 
		PERIOD_NAME,
		PERIOD_COUNTER,
		FISCAL_YEAR,
		PERIOD_NUM,
		PERIOD_OPEN_DATE,
		PERIOD_CLOSE_DATE,
		DEPRECIATION_BATCH_ID,
		RETIREMENT_BATCH_ID,
		RECLASS_BATCH_ID,
		TRANSFER_BATCH_ID,
		ADDITION_BATCH_ID,
		ADJUSTMENT_BATCH_ID,
		DEFERRED_DEPRN_BATCH_ID,
		CALENDAR_PERIOD_OPEN_DATE,
		CALENDAR_PERIOD_CLOSE_DATE,
		CIP_ADDITION_BATCH_ID,
		CIP_ADJUSTMENT_BATCH_ID,
		CIP_RECLASS_BATCH_ID,
		CIP_RETIREMENT_BATCH_ID,
		CIP_REVAL_BATCH_ID,
		CIP_TRANSFER_BATCH_ID,
		REVAL_BATCH_ID,
		DEPRN_ADJUSTMENT_BATCH_ID,
		DEPRN_RUN,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.FA_MC_DEPRN_PERIODS
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(BOOK_TYPE_CODE,'NA'),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0),kca_seq_id) in 
	(select nvl(BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE,nvl(PERIOD_COUNTER,0) as PERIOD_COUNTER,nvl(SET_OF_BOOKS_ID,0) as SET_OF_BOOKS_ID,max(kca_seq_id) from bec_ods_stg.FA_MC_DEPRN_PERIODS 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(BOOK_TYPE_CODE,'NA'),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0))
);

commit;

-- Soft delete
update bec_ods.FA_MC_DEPRN_PERIODS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FA_MC_DEPRN_PERIODS set IS_DELETED_FLG = 'Y'
where (nvl(BOOK_TYPE_CODE,'NA'),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0))  in
(
select nvl(BOOK_TYPE_CODE,'NA'),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0) from bec_raw_dl_ext.FA_MC_DEPRN_PERIODS
where (nvl(BOOK_TYPE_CODE,'NA'),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0),KCA_SEQ_ID)
in 
(
select nvl(BOOK_TYPE_CODE,'NA'),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FA_MC_DEPRN_PERIODS
group by nvl(BOOK_TYPE_CODE,'NA'),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0)
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'fa_mc_deprn_periods';

commit;