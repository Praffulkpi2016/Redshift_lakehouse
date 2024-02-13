/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Facts.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh.FACT_FA_ASSET_LIST;

create table bec_dwh.FACT_FA_ASSET_LIST 
	diststyle all
	sortkey (ASSET_ID,SET_OF_BOOKS_ID)
as
select
	A.PERIOD_NAME,
	A.PRORATE_DATE,
	A.SET_OF_BOOKS_ID,
	A.ORIGINAL_COST,
	A.SALVAGE_VALUE,
	A.BOOK_TYPE_CODE,
	A.ASSET_ID,
	A.DATE_PLACED_IN_SERVICE,
	A.DEPRN_METHOD_CODE,
	A.LIFE_MONTHS,
	A.LIFE_YEARS,
	A.REMAINDER_LIFE_MONTHS,
	A.REMAINDER_LIFE_YEARS,
	A.FB_PRORATE_DATE,
	A.COST,
	A.DEPRECIATE_FLAG,
	A.RETIREMENT_ID,
	A.PERIOD_COUNTER,
	A.DEPRN_SOURCE_CODE,
	A.DEPRN_RUN_DATE,
	A.DEPRN_AMOUNT,
	A.YTD_DEPRN,
	A.DEPRN_RESERVE,
	A.ADDITION_COST_TO_CLEAR,
	A.NET_BOOK_VALUE,
	A.DIST_COST,
	A.DEPRN_ADJUSTMENT_AMOUNT,
	A.BONUS_DEPRN_ADJUSTMENT_AMOUNT,
	A.CATEGORY_ID,
	A.UNITS_ASSIGNED,
	A.LOCATION_ID,
	A.ASSIGNED_TO,
	A.DISTRIBUTION_ID,
	A.DEPRN_AMOUNT_SUM,
	A.YTD_DERPN_SUM,
	A.DEPRN_RESERVE_SUM,
	A.NET_BOOK_VALUE_SUM,
	A.DATE_EFFECTIVE,
	A.CODE_COMBINATION_ID,
	A.DATE_EFFECTIVE_FB,
	A.DATE_EFFECTIVE_FDH ,
	A.TRANSACTION_HEADER_ID_IN,
	A.ASSET_TYPE,
	'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || A.SET_OF_BOOKS_ID as SET_OF_BOOKS_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || A.ASSET_ID as ASSET_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || A.RETIREMENT_ID as RETIREMENT_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || A.CATEGORY_ID as CATEGORY_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || A.LOCATION_ID as LOCATION_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || A.DISTRIBUTION_ID as DISTRIBUTION_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || A.CODE_COMBINATION_ID as CODE_COMBINATION_ID_KEY,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || A.TRANSACTION_HEADER_ID_IN as TRANSACTION_HEADER_ID_IN_KEY,
			(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' ||
	nvl(A.asset_id, 0)|| '-' || nvl(A.period_name, 'NA')|| '-' || nvl(A.set_of_books_id, 0)|| '-' ||
	nvl(A.period_counter, 0)|| '-' || nvl(A.transaction_header_id_in, 0)|| '-' ||
	nvl(A.distribution_id, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	(
	select
		fdp.PERIOD_NAME,
		PRORATE_DATE,
		fbc.set_of_books_id,
		fb.ORIGINAL_COST,
		fb.SALVAGE_VALUE,
		fb.BOOK_TYPE_CODE,
		fb.ASSET_ID,
		fb.DATE_PLACED_IN_SERVICE,
		fb.DEPRN_METHOD_CODE,
		fb.LIFE_IN_MONTHS as LIFE_MONTHS,
		fb.LIFE_IN_MONTHS / 12 ::numeric(20,2) as LIFE_YEARS,
		fb.LIFE_IN_MONTHS - datediff (month,TRUNC (fb.PRORATE_DATE),TRUNC (TO_DATE (fdp.PERIOD_NAME ,'MON YY')) ) as REMAINDER_LIFE_MONTHS,
		(fb.LIFE_IN_MONTHS - datediff (month,TRUNC (fb.PRORATE_DATE),TRUNC(TO_DATE (fdp.PERIOD_NAME ,'MON YY'))))/12::numeric(20,2) as REMAINDER_LIFE_YEARS,
		fb.PRORATE_DATE fb_PRORATE_DATE,
		fb.cost,
		fb.DEPRECIATE_FLAG,
		fb.RETIREMENT_ID,
		fdp.PERIOD_COUNTER,
		fdd.DEPRN_SOURCE_CODE,
		fdd.DEPRN_RUN_DATE,
		DECODE (fdp.period_counter - fdd.period_counter,0,fdd.DEPRN_AMOUNT,0) as DEPRN_AMOUNT,
		DECODE (SUBSTRING (fdp.period_name,5,2),SUBSTRING (fdp3.period_name,5,2),fdd.YTD_DEPRN,0) as ytd_deprn,
		fdd.DEPRN_RESERVE,
		fdd.ADDITION_COST_TO_CLEAR,
		DECODE (fdd.DEPRN_SOURCE_CODE,'B',fdd.ADDITION_COST_TO_CLEAR,fdd.COST) - NVL (fdd.DEPRN_RESERVE,0) as Net_Book_Value,
		DECODE (fdd.DEPRN_SOURCE_CODE,'B',fdd.ADDITION_COST_TO_CLEAR,fdd.COST) as dist_Cost,
		fdd.DEPRN_ADJUSTMENT_AMOUNT,
		fdd.BONUS_DEPRN_ADJUSTMENT_AMOUNT,
		fah.category_id,
		fdh.units_assigned,
		fdh.LOCATION_ID,
		NVL (fdh.ASSIGNED_TO,- 23453) as ASSIGNED_TO,
		fdh.DISTRIBUTION_ID,
		DECODE (fdp.period_counter - fds.period_counter,0,fds.DEPRN_AMOUNT,0) as DEPRN_AMOUNT_SUM,
		DECODE (SUBSTRING (fdp.period_name,5,2),SUBSTRING (fdp2.period_name,5,2),fds.YTD_DEPRN,0) as YTD_DERPN_SUM,
		FDS.DEPRN_RESERVE as DEPRN_RESERVE_SUM,
		FB.COST - NVL (FDS.DEPRN_RESERVE,0) as Net_Book_Value_SUM,
		fah.DATE_EFFECTIVE,
		fdh.CODE_COMBINATION_ID,
		fb.date_effective as date_effective_fb,
		fdh.date_effective as date_effective_fdh ,
		fb.TRANSACTION_HEADER_ID_IN,
		fab.asset_type
	from
		(select * from bec_ods.fa_books where is_deleted_flg <> 'Y') fb
	inner join (select * from bec_ods.fa_book_controls where is_deleted_flg <> 'Y') fbc on
		fb.book_type_code = fbc.book_type_code
	inner join (select * from bec_ods.fa_deprn_detail where is_deleted_flg <> 'Y') fdd on
		fb.book_type_code = fdd.book_type_code
		and fb.asset_id = fdd.asset_id
	inner join (
		select
			PERIOD_NAME,
						PERIOD_COUNTER,
						period_close_date,
						book_type_code,
						period_open_date
		from
			(select * from bec_ods.fa_deprn_periods where is_deleted_flg <> 'Y')
        ) fdp on
		fb.book_type_code = fdp.book_type_code
	inner join (select * from bec_ods.fa_additions_b where is_deleted_flg <> 'Y') fab on
		fb.asset_id = fab.asset_id
	inner join (select * from bec_ods.fa_asset_history where is_deleted_flg <> 'Y') fah on
		fab.asset_id = fah.asset_id
	inner join (select * from bec_ods.fa_distribution_history where is_deleted_flg <> 'Y') fdh on
		fdh.asset_id = fb.asset_id
		and fdd.distribution_id = fdh.distribution_id
	inner join (select * from bec_ods.fa_deprn_summary where is_deleted_flg <> 'Y') fds on
		fb.book_type_code = fds.book_type_code
		and fb.asset_id = fds.asset_id
		and fdd.period_counter = fds.period_counter
	inner join (select * from bec_ods.fa_deprn_periods where is_deleted_flg <> 'Y') fdp2 on
		fdp2.period_counter = fds.period_counter
		and fdp2.book_type_code = fds.book_type_code
	inner join (select * from bec_ods.fa_deprn_periods where is_deleted_flg <> 'Y') fdp3 on
		fdp3.period_counter = fdd.period_counter
		and fdp3.book_type_code = fdd.book_type_code
	where
		fb.date_effective <= NVL (fdp.period_close_date,getdate())
		and NVL (fb.date_ineffective,getdate()) >= NVL (fdp.period_close_date,getdate())
		and (fdh.date_effective <= NVL (fdp.period_close_date,getdate())
		and NVL (fdh.date_ineffective,getdate()) >= NVL (fdp.period_close_date,getdate())
		or NVL (fdh.date_ineffective,getdate()) >= fdp.period_open_date
		and NVL (fdh.date_ineffective,getdate()) <= fdp.period_close_date)
		and fdd.period_counter =
			(select MAX (period_counter)
					from
						(select * from bec_ods.fa_deprn_detail where is_deleted_flg <> 'Y') dd
					where
						dd.book_type_code = fdp.book_type_code
						and dd.asset_id = fb.asset_id
						and dd.distribution_id = fdd.distribution_id
						and dd.period_counter <= fdp.period_counter )
		and fah.date_effective <= NVL (fdp.period_close_date,getdate())
		and NVL (fah.date_ineffective,getdate()) >= NVL (fdp.period_close_date,getdate())
		and NVL (fb.period_counter_fully_retired,fdp.period_counter + 1) >= fdp.period_counter
	union all
	select
		fdp.PERIOD_NAME,
		PRORATE_DATE,
		fbc.set_of_books_id,
		fb.ORIGINAL_COST,
		fb.SALVAGE_VALUE,
		fb.BOOK_TYPE_CODE,
		fb.ASSET_ID,
		fb.DATE_PLACED_IN_SERVICE,
		fb.DEPRN_METHOD_CODE,
		fb.LIFE_IN_MONTHS as LIFE_MONTHS,
		fb.LIFE_IN_MONTHS / 12 ::numeric(20,2) as LIFE_YEARS,
		fb.LIFE_IN_MONTHS - datediff (month,TRUNC (fb.PRORATE_DATE),TRUNC (TO_DATE (fdp.PERIOD_NAME ,'MON YY')) ) as REMAINDER_LIFE_MONTHS,
		(fb.LIFE_IN_MONTHS - datediff (month,TRUNC (fb.PRORATE_DATE),TRUNC (TO_DATE (fdp.PERIOD_NAME ,'MON YY')) ) ) / 12::numeric(20,2) as REMAINDER_LIFE_YEARS,
		fb.PRORATE_DATE,
		fb.cost,
		fb.DEPRECIATE_FLAG,
		fb.RETIREMENT_ID,
		fdp.PERIOD_COUNTER,
		fdd.DEPRN_SOURCE_CODE,
		fdd.DEPRN_RUN_DATE,
		DECODE (fdp.period_counter - fdd.period_counter,0,fdd.DEPRN_AMOUNT,0) as DEPRN_AMOUNT,
		DECODE (SUBSTRING (fdp.period_name,5,2),SUBSTRING (fdp3.period_name,5,2),fdd.YTD_DEPRN,0) as ytd_deprn,
		fdd.DEPRN_RESERVE,
		fdd.ADDITION_COST_TO_CLEAR,
		DECODE (fdd.DEPRN_SOURCE_CODE,'B',fdd.ADDITION_COST_TO_CLEAR,fdd.COST) - NVL (fdd.DEPRN_RESERVE,0) as Net_Book_Value,
		DECODE (fdd.DEPRN_SOURCE_CODE,'B',fdd.ADDITION_COST_TO_CLEAR,fdd.COST) as dist_Cost,
		fdd.DEPRN_ADJUSTMENT_AMOUNT,
		fdd.BONUS_DEPRN_ADJUSTMENT_AMOUNT,
		fah.category_id,
		fdh.units_assigned,
		fdh.LOCATION_ID,
		NVL (fdh.ASSIGNED_TO,- 23453) as ASSIGNED_TO,
		fdh.DISTRIBUTION_ID,
		DECODE (fdp.period_counter - fds.period_counter,0,fds.DEPRN_AMOUNT,0) as DEPRN_AMOUNT_SUM,
		DECODE (SUBSTRING (fdp.period_name,5,2),SUBSTRING (fdp2.period_name,5,2),fds.YTD_DEPRN,0) as YTD_DERPN_SUM,
		FDS.DEPRN_RESERVE as DEPRN_RESERVE_SUM,
		FB.COST - NVL (FDS.DEPRN_RESERVE,0) as Net_Book_Value_SUM,
		fah.DATE_EFFECTIVE,
		fdh.CODE_COMBINATION_ID,
		fb.date_effective as date_effective_fb,
		fdh.date_effective as date_effective_fdh ,
		fb.TRANSACTION_HEADER_ID_IN,
		fab.asset_type
	from
		(select * from bec_ods.fa_mc_books where is_deleted_flg <> 'Y') fb
	inner join (select * from bec_ods.fa_mc_book_controls where is_deleted_flg <> 'Y') fbc on
		fb.book_type_code = fbc.book_type_code
	inner join (select * from bec_ods.fa_mc_deprn_detail where is_deleted_flg <> 'Y') fdd on
		fb.book_type_code = fdd.book_type_code
		and fb.asset_id = fdd.asset_id
	inner join (
		select
			PERIOD_NAME,
			PERIOD_COUNTER,
			period_close_date,
			book_type_code,
			period_open_date
		from
			(select * from bec_ods.fa_mc_deprn_periods where is_deleted_flg <> 'Y')
   ) fdp on
		fb.book_type_code = fdp.book_type_code
	inner join (select * from bec_ods.fa_additions_b  where is_deleted_flg <> 'Y')fab on
		fb.asset_id = fab.asset_id
	inner join (select * from bec_ods.fa_asset_history  where is_deleted_flg <> 'Y')fah on
		fab.asset_id = fah.asset_id
	inner join (select * from bec_ods.fa_distribution_history where is_deleted_flg <> 'Y') fdh on
		fdh.asset_id = fb.asset_id
		and fdd.distribution_id = fdh.distribution_id
	inner join (select * from bec_ods.fa_mc_deprn_summary where is_deleted_flg <> 'Y') fds on
		fb.book_type_code = fds.book_type_code
		and fb.asset_id = fds.asset_id
		and fdd.period_counter = fds.period_counter
	inner join (select * from bec_ods.fa_mc_deprn_periods where is_deleted_flg <> 'Y') fdp2 on
		fdp2.period_counter = fds.period_counter
		and fdp2.book_type_code = fds.book_type_code
	inner join (select * from bec_ods.fa_mc_deprn_periods where is_deleted_flg <> 'Y') fdp3 on
		fdp3.period_counter = fdd.period_counter
		and fdp3.book_type_code = fdd.book_type_code
	where
		fb.date_effective <= NVL (fdp.period_close_date,getdate())
		and NVL (fb.date_ineffective,getdate()) >= NVL (fdp.period_close_date,getdate())
		and (fdh.date_effective <= NVL (fdp.period_close_date,getdate())
		and NVL (fdh.date_ineffective,getdate()) >= NVL (fdp.period_close_date,getdate())
		or NVL (fdh.date_ineffective,getdate()) >= fdp.period_open_date
		and NVL (fdh.date_ineffective,getdate()) <= fdp.period_close_date)
		and fdd.period_counter =
				(select
						MAX (period_counter)
					from
						(select * from bec_ods.fa_mc_deprn_detail where is_deleted_flg <> 'Y') dd
					where
						dd.book_type_code = fdp.book_type_code
						and dd.asset_id = fb.asset_id
						and dd.distribution_id = fdd.distribution_id
						and dd.period_counter <= fdp.period_counter
				 )
		and fah.date_effective <= NVL (fdp.period_close_date,getdate())
		and NVL (fah.date_ineffective,getdate()) >= NVL (fdp.period_close_date,getdate())
		and NVL (fb.period_counter_fully_retired,fdp.period_counter + 1) >= fdp.period_counter
) A;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_fa_asset_list'
	and batch_name = 'fa';

commit;