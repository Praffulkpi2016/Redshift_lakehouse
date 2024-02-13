/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;
-- Delete Records
delete
from
	bec_dwh.DIM_ASSET_PERIOD
where
	(nvl(SET_OF_BOOKS_ID, 0),
	nvl(BOOK_TYPE_CODE, 'NA'),
	nvl(PERIOD_COUNTER, 0)) in
(
	select
		nvl(ods.SET_OF_BOOKS_ID, 0) as SET_OF_BOOKS_ID,
		nvl(ods.BOOK_TYPE_CODE, 'NA') as BOOK_TYPE_CODE,
		nvl(ods.PERIOD_COUNTER, 0) as PERIOD_COUNTER
	from
		bec_dwh.DIM_ASSET_PERIOD dw,
		(
		select
			FA_BOOK_CONTROLS.SET_OF_BOOKS_ID SET_OF_BOOKS_ID,
			FA_DEPRN_PERIODS.BOOK_TYPE_CODE BOOK_TYPE_CODE,
			FA_DEPRN_PERIODS.PERIOD_COUNTER PERIOD_COUNTER
		from
			bec_ods.FA_DEPRN_PERIODS FA_DEPRN_PERIODS
		inner join bec_ods.FA_BOOK_CONTROLS FA_BOOK_CONTROLS
	on
			FA_DEPRN_PERIODS.BOOK_TYPE_CODE = FA_BOOK_CONTROLS.BOOK_TYPE_CODE
			and (FA_BOOK_CONTROLS.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_asset_period'
				and batch_name = 'fa')
				 )
	union all
		select
			FA_MC_DEPRN_PERIODS.set_of_books_id SET_OF_BOOKS_ID,
			FA_MC_DEPRN_PERIODS.BOOK_TYPE_CODE BOOK_TYPE_CODE,
			FA_MC_DEPRN_PERIODS.PERIOD_COUNTER PERIOD_COUNTER

		from
			bec_ods.FA_MC_BOOK_CONTROLS FA_MC_BOOK_CONTROLS
		inner join bec_ods.FA_MC_DEPRN_PERIODS FA_MC_DEPRN_PERIODS
		on
			FA_MC_BOOK_CONTROLS.BOOK_TYPE_CODE = FA_MC_DEPRN_PERIODS.BOOK_TYPE_CODE
			and 
			(FA_MC_BOOK_CONTROLS.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_asset_period'
				and batch_name = 'fa')
				 )
			) ods
where
		dw.dw_load_id = 
						(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )
    || '-'
    || nvl(ods.SET_OF_BOOKS_ID, 0)
	|| '-'
	|| nvl(ods.BOOK_TYPE_CODE, 'NA')
	|| '-'
    || nvl(ods.PERIOD_COUNTER, 0)
);

commit;
-- Insert Records
insert
	into
	bec_dwh.DIM_ASSET_PERIOD
(
	book_type_code,
	period_name,
	period_counter,
	fiscal_year,
	period_num,
	period_open_date,
	period_close_date,
	set_of_books_id,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
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
	set_of_books_id,
	'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    ) as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )
    || '-'
    || nvl(SET_OF_BOOKS_ID, 0)
	|| '-'
	|| nvl(BOOK_TYPE_CODE, 'NA')
	|| '-'
    || nvl(PERIOD_COUNTER, 0)as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	(select
		FA_DEPRN_PERIODS.BOOK_TYPE_CODE BOOK_TYPE_CODE,
			FA_DEPRN_PERIODS.PERIOD_NAME PERIOD_NAME,
			FA_DEPRN_PERIODS.PERIOD_COUNTER PERIOD_COUNTER,
			FA_DEPRN_PERIODS.FISCAL_YEAR FISCAL_YEAR,
			FA_DEPRN_PERIODS.PERIOD_NUM PERIOD_NUM,
			FA_DEPRN_PERIODS.PERIOD_OPEN_DATE PERIOD_OPEN_DATE,
			FA_DEPRN_PERIODS.PERIOD_CLOSE_DATE PERIOD_CLOSE_DATE,
			FA_BOOK_CONTROLS.SET_OF_BOOKS_ID SET_OF_BOOKS_ID
	from
		bec_ods.FA_DEPRN_PERIODS FA_DEPRN_PERIODS
	inner join bec_ods.FA_BOOK_CONTROLS FA_BOOK_CONTROLS
	on
		FA_DEPRN_PERIODS.BOOK_TYPE_CODE = FA_BOOK_CONTROLS.BOOK_TYPE_CODE
		and (FA_BOOK_CONTROLS.kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_dw_info
		where
			dw_table_name = 'dim_asset_period'
			and batch_name = 'fa')
			 )
union all
	select
		FA_MC_DEPRN_PERIODS.BOOK_TYPE_CODE BOOK_TYPE_CODE,
		FA_MC_DEPRN_PERIODS.PERIOD_NAME PERIOD_NAME,
		FA_MC_DEPRN_PERIODS.PERIOD_COUNTER PERIOD_COUNTER,
		FA_MC_DEPRN_PERIODS.FISCAL_YEAR FISCAL_YEAR,
		FA_MC_DEPRN_PERIODS.PERIOD_NUM PERIOD_NUM,
		FA_MC_DEPRN_PERIODS.PERIOD_OPEN_DATE PERIOD_OPEN_DATE,
		FA_MC_DEPRN_PERIODS.PERIOD_CLOSE_DATE PERIOD_CLOSE_DATE,
		FA_MC_DEPRN_PERIODS.set_of_books_id SET_OF_BOOKS_ID
	from
		bec_ods.FA_MC_BOOK_CONTROLS FA_MC_BOOK_CONTROLS
	inner join bec_ods.FA_MC_DEPRN_PERIODS FA_MC_DEPRN_PERIODS
on
		FA_MC_BOOK_CONTROLS.BOOK_TYPE_CODE = FA_MC_DEPRN_PERIODS.BOOK_TYPE_CODE
		and (FA_MC_BOOK_CONTROLS.kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_dw_info
		where
			dw_table_name = 'dim_asset_period'
			and batch_name = 'fa')
			 )
)
);
-- Soft Delete
update
	bec_dwh.DIM_ASSET_PERIOD
set
	is_deleted_flg = 'Y'
where
	(nvl(SET_OF_BOOKS_ID, 0),
	nvl(BOOK_TYPE_CODE, 'NA'),
	nvl(PERIOD_COUNTER, 0)) not in 
(
	select
		nvl(ods.SET_OF_BOOKS_ID, 0) as SET_OF_BOOKS_ID,
		nvl(ods.BOOK_TYPE_CODE, 'NA') as BOOK_TYPE_CODE,
		nvl(ods.PERIOD_COUNTER, 0) as PERIOD_COUNTER
	from
		bec_dwh.DIM_ASSET_PERIOD dw,
		(
		select
			FA_BOOK_CONTROLS.SET_OF_BOOKS_ID SET_OF_BOOKS_ID,
			FA_DEPRN_PERIODS.BOOK_TYPE_CODE BOOK_TYPE_CODE,
			FA_DEPRN_PERIODS.PERIOD_COUNTER PERIOD_COUNTER
		from
			(select * from bec_ods.FA_DEPRN_PERIODS 
				where is_deleted_flg <> 'Y') FA_DEPRN_PERIODS
		inner join (select * from bec_ods.FA_BOOK_CONTROLS 
				where is_deleted_flg <> 'Y') FA_BOOK_CONTROLS
	on
			FA_DEPRN_PERIODS.BOOK_TYPE_CODE = FA_BOOK_CONTROLS.BOOK_TYPE_CODE
	union all
		select
			FA_MC_DEPRN_PERIODS.set_of_books_id SET_OF_BOOKS_ID,
			FA_MC_DEPRN_PERIODS.BOOK_TYPE_CODE BOOK_TYPE_CODE,
			FA_MC_DEPRN_PERIODS.PERIOD_COUNTER PERIOD_COUNTER
		from
			(select * from bec_ods.FA_MC_BOOK_CONTROLS 
				where is_deleted_flg <> 'Y') FA_MC_BOOK_CONTROLS
		inner join (select * from bec_ods.FA_MC_DEPRN_PERIODS 
				where is_deleted_flg <> 'Y') FA_MC_DEPRN_PERIODS
		on
			FA_MC_BOOK_CONTROLS.BOOK_TYPE_CODE = FA_MC_DEPRN_PERIODS.BOOK_TYPE_CODE
			) ods
where
		dw.dw_load_id = 
						(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
    )
    || '-'
    || nvl(ods.SET_OF_BOOKS_ID, 0)
	|| '-'
	|| nvl(ods.BOOK_TYPE_CODE, 'NA')
	|| '-'
    || nvl(ods.PERIOD_COUNTER, 0)
);

commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_asset_period'
	and batch_name = 'fa';

commit;