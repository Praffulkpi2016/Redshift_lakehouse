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
	bec_ods.FA_MC_BOOK_CONTROLS
where
	(nvl(BOOK_TYPE_CODE,'NA'),nvl(SET_OF_BOOKS_ID,0)) in (
	select
		nvl(stg.BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE,nvl(stg.SET_OF_BOOKS_ID,0) as SET_OF_BOOKS_ID
	from
		bec_ods.FA_MC_BOOK_CONTROLS ods,
		bec_ods_stg.FA_MC_BOOK_CONTROLS stg
	where
		nvl(ods.BOOK_TYPE_CODE,'NA') = nvl(stg.BOOK_TYPE_CODE,'NA')
		and nvl(ods.SET_OF_BOOKS_ID,0) = nvl(stg.SET_OF_BOOKS_ID,0)
		and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.FA_MC_BOOK_CONTROLS
       (
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
	kca_operation,
	IS_DELETED_FLG,
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
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.FA_MC_BOOK_CONTROLS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (nvl(BOOK_TYPE_CODE,'NA'),nvl(SET_OF_BOOKS_ID,0),
		kca_seq_id) in 
	(
		select
			nvl(BOOK_TYPE_CODE,'NA') as BOOK_TYPE_CODE,nvl(SET_OF_BOOKS_ID,0) as SET_OF_BOOKS_ID,
			max(kca_seq_id)
		from
			bec_ods_stg.FA_MC_BOOK_CONTROLS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			nvl(BOOK_TYPE_CODE,'NA'),nvl(SET_OF_BOOKS_ID,0))
);

commit;

-- Soft delete
update bec_ods.FA_MC_BOOK_CONTROLS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FA_MC_BOOK_CONTROLS set IS_DELETED_FLG = 'Y'
where (nvl(BOOK_TYPE_CODE,'NA'),nvl(SET_OF_BOOKS_ID,0))  in
(
select nvl(BOOK_TYPE_CODE,'NA'),nvl(SET_OF_BOOKS_ID,0) from bec_raw_dl_ext.FA_MC_BOOK_CONTROLS
where (nvl(BOOK_TYPE_CODE,'NA'),nvl(SET_OF_BOOKS_ID,0),KCA_SEQ_ID)
in 
(
select nvl(BOOK_TYPE_CODE,'NA'),nvl(SET_OF_BOOKS_ID,0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FA_MC_BOOK_CONTROLS
group by nvl(BOOK_TYPE_CODE,'NA'),nvl(SET_OF_BOOKS_ID,0)
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
	ods_table_name = 'fa_mc_book_controls';

commit;