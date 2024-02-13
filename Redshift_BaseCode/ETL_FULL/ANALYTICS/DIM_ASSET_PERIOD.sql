/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh.DIM_ASSET_PERIOD;

create table bec_dwh.DIM_ASSET_PERIOD
	diststyle all sortkey(SET_OF_BOOKS_ID,BOOK_TYPE_CODE,PERIOD_COUNTER)
as
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
    || nvl(PERIOD_COUNTER, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	(
	select
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
)
);
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