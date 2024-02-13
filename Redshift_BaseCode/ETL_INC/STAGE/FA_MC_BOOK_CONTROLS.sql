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
	table bec_ods_stg.FA_MC_BOOK_CONTROLS;

insert
	into
	bec_ods_stg.FA_MC_BOOK_CONTROLS
(set_of_books_id,
	book_type_code,
	currency_code,
	deprn_status,
	deprn_request_id,
	last_period_counter,
	last_deprn_run_date,
	current_fiscal_year,
	retired_status,
	retired_request_id,
	primary_set_of_books_id,
	primary_currency_code,
	source_retired_status,
	source_retired_request_id,
	mrc_converted_flag,
	enabled_flag,
	nbv_amount_threshold,
	conversion_status,
	last_updated_by,
	last_update_date,
	last_update_login,
	mass_request_id,
	allow_impairment_flag,
	gl_posting_allowed_flag,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
)
(
	select
		set_of_books_id,
	book_type_code,
	currency_code,
	deprn_status,
	deprn_request_id,
	last_period_counter,
	last_deprn_run_date,
	current_fiscal_year,
	retired_status,
	retired_request_id,
	primary_set_of_books_id,
	primary_currency_code,
	source_retired_status,
	source_retired_request_id,
	mrc_converted_flag,
	enabled_flag,
	nbv_amount_threshold,
	conversion_status,
	last_updated_by,
	last_update_date,
	last_update_login,
	mass_request_id,
	allow_impairment_flag,
	gl_posting_allowed_flag,
		KCA_OPERATION,
		kca_seq_id,
		kca_seq_date
	from
		bec_raw_dl_ext.FA_MC_BOOK_CONTROLS
	where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		and (nvl(BOOK_TYPE_CODE,'NA'),nvl(SET_OF_BOOKS_ID,0),
		kca_seq_id) in 
(
		select
			nvl(BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE,nvl(SET_OF_BOOKS_ID,0) as SET_OF_BOOKS_ID,
			max(kca_seq_id)
		from
			bec_raw_dl_ext.FA_MC_BOOK_CONTROLS
		where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
		group by
			nvl(BOOK_TYPE_CODE,'NA'),nvl(SET_OF_BOOKS_ID,0))
		and 
kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'fa_mc_book_controls')
);
end;