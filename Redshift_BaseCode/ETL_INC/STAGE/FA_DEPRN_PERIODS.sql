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
	table bec_ods_stg.FA_DEPRN_PERIODS;

insert
	into
	bec_ods_stg.FA_DEPRN_PERIODS
(book_type_code,
	period_name,
	period_counter,
	fiscal_year,
	period_num,
	period_open_date,
	period_close_date,
	depreciation_batch_id,
	retirement_batch_id,
	reclass_batch_id,
	transfer_batch_id,
	addition_batch_id,
	adjustment_batch_id,
	deferred_deprn_batch_id,
	calendar_period_open_date,
	calendar_period_close_date,
	cip_addition_batch_id,
	cip_adjustment_batch_id,
	cip_reclass_batch_id,
	cip_retirement_batch_id,
	cip_reval_batch_id,
	cip_transfer_batch_id,
	reval_batch_id,
	deprn_adjustment_batch_id,
	deprn_run,
	xla_conversion_status,
	gl_transfer_flag,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		book_type_code,
	period_name,
	period_counter,
	fiscal_year,
	period_num,
	period_open_date,
	period_close_date,
	depreciation_batch_id,
	retirement_batch_id,
	reclass_batch_id,
	transfer_batch_id,
	addition_batch_id,
	adjustment_batch_id,
	deferred_deprn_batch_id,
	calendar_period_open_date,
	calendar_period_close_date,
	cip_addition_batch_id,
	cip_adjustment_batch_id,
	cip_reclass_batch_id,
	cip_retirement_batch_id,
	cip_reval_batch_id,
	cip_transfer_batch_id,
	reval_batch_id,
	deprn_adjustment_batch_id,
	deprn_run,
	xla_conversion_status,
	gl_transfer_flag,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.FA_DEPRN_PERIODS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(BOOK_TYPE_CODE,'NA'),nvl(PERIOD_COUNTER,0),
		kca_seq_id) in 
(
		select
			nvl(BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE,nvl(PERIOD_COUNTER,0) as PERIOD_COUNTER,
			max(kca_seq_id)
		from
			bec_raw_dl_ext.FA_DEPRN_PERIODS
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(BOOK_TYPE_CODE,'NA'),nvl(PERIOD_COUNTER,0))		
		and	kca_seq_date  > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fa_deprn_periods')
);
end;