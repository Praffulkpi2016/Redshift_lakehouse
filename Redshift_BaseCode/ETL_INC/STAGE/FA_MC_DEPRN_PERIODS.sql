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

truncate table bec_ods_stg.FA_MC_DEPRN_PERIODS;

insert into	bec_ods_stg.FA_MC_DEPRN_PERIODS
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
		kca_seq_id,
		kca_seq_date
	from bec_raw_dl_ext.FA_MC_DEPRN_PERIODS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (nvl(BOOK_TYPE_CODE,'NA'),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0),kca_seq_id) in 
	(select nvl(BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE,nvl(PERIOD_COUNTER,0) as PERIOD_COUNTER,nvl(SET_OF_BOOKS_ID,0) as SET_OF_BOOKS_ID,max(kca_seq_id) from bec_raw_dl_ext.FA_MC_DEPRN_PERIODS 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by nvl(BOOK_TYPE_CODE,'NA'),nvl(PERIOD_COUNTER,0),nvl(SET_OF_BOOKS_ID,0))
        and	kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fa_mc_deprn_periods')
);
end;