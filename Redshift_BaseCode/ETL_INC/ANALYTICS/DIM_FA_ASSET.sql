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
	bec_dwh.DIM_FA_ASSET
where
	(nvl(asset_id, 0),
	nvl(book_type_code, 'NA'),
	nvl(period_retired_name,'NA')) in (
	select
		nvl(ods.asset_id, 0) as asset_id,
		nvl(ods.book_type_code, 'NA') as book_type_code,
		nvl(ods.period_name,'NA') as period_retired_name
	from
		bec_dwh.DIM_FA_ASSET dw,
		(
select 
	fa_additions_b.asset_id,
	fa_books.book_type_code,
	fa_deprn_periods.period_name
from bec_ods.fa_additions_b fa_additions_b 
left join bec_ods.fa_additions_tl fa_additions_tl 
on 
	fa_additions_b.asset_id = fa_additions_tl.asset_id 
	and fa_additions_tl.language = 'US'
inner join bec_ods.fa_categories_b fa_categories_b 
on 
	fa_categories_b.category_id = fa_additions_b.asset_category_id 
inner join bec_ods.fa_books fa_books 
on 
	fa_books.asset_id = fa_additions_b.asset_id
AND fa_books.date_ineffective IS NULL --added to consider only active rows	
inner join bec_ods.fa_categories_tl fa_categories_tl 
on 
	fa_categories_tl.category_id = fa_additions_b.asset_category_id 
	and fa_categories_tl.language = 'US'
left join bec_ods.fa_deprn_periods fa_deprn_periods 
on 
	fa_deprn_periods.period_counter = fa_books.period_counter_fully_retired
	and fa_deprn_periods.book_type_code = fa_books.book_type_code
--added tables
inner join bec_ods.fa_category_books fa_category_books
on  fa_category_books.book_type_code = fa_books.book_type_code 
AND fa_category_books.CATEGORY_ID = fa_additions_b.asset_category_id
		where
			(
		fa_additions_b.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_asset'
				and batch_name = 'fa')				 
			or fa_books.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_asset'
				and batch_name = 'fa')
			or fa_deprn_periods.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_asset'
				and batch_name = 'fa')
				 )
) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ods.asset_id, 0)|| '-' || nvl(ods.book_type_code, 'NA')
			||'-'||nvl(ods.period_name,'NA')
			);

commit;
-- Insert records

insert
	into
	bec_dwh.DIM_FA_ASSET
(
asset_id,
	book_type_code,
	asset_number,
	asset_key_ccid,
	asset_type,
	tag_number,
	parent_asset_id,
	manufacturer_name,
	serial_number,
	model_number,
	property_type_code,
	in_use_flag,
	owned_leased,
	new_used,
	additions_b_last_upd_dt,
	last_updated_by,
	created_by,
	creation_date,
	asset_description,
	additions_tl_last_upd_dt,
	asset_major_category,
	asset_minor_category,
	date_placed_in_service,
	x_custom,
	life_in_months,
	deprn_method,
	grp_asset_id,
	asset_subtype,
	current_units,
	asset_category,
	asset_category_desc,
	depreciate_flg,
	cat_b_last_upd_dt,
	cat_tl_last_upd_dt,
	books_last_upd_dt,
	period_retired_name,
	original_cost,
	PRODUCTION_CAPACITY,
	ADJUSTED_RATE,
	GL_ACCOUNT,
	RESERVE_ACCOUNT,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
select 
	fa_additions_b.asset_id,
	fa_books.book_type_code,
	fa_additions_b.asset_number,
	fa_additions_b.asset_key_ccid,
	fa_additions_b.asset_type,
	fa_additions_b.tag_number,
	fa_additions_b.parent_asset_id,
	fa_additions_b.manufacturer_name,
	fa_additions_b.serial_number,
	fa_additions_b.model_number,
	fa_additions_b.property_type_code,
	fa_additions_b.in_use_flag,
	fa_additions_b.owned_leased,
	fa_additions_b.new_used,
	fa_additions_b.last_update_date additions_b_last_upd_dt,
	fa_additions_b.last_updated_by,
	fa_additions_b.created_by,
	fa_additions_b.creation_date,
	fa_additions_tl.description as asset_description,
	fa_additions_tl.last_update_date additions_tl_last_upd_dt,
	fa_categories_b.segment1 as asset_major_category,
	fa_categories_b.segment2 as asset_minor_category,
	fa_books.date_placed_in_service date_placed_in_service,
	'0' as x_custom,
	fa_books.life_in_months life_in_months,
	fa_books.deprn_method_code deprn_method,
	fa_books.group_asset_id grp_asset_id,
	case
		when (fa_additions_b.asset_type = 'GROUP' and fa_books.group_asset_id is null) then 'GROUP'
		when (fa_additions_b.asset_type = 'CAPITALIZED' and fa_books.group_asset_id is not null) then 'MEMBER'
		when (fa_additions_b.asset_type = 'CAPITALIZED' and fa_books.group_asset_id is null) then 'STANDALONE'
	end asset_subtype,
	fa_additions_b.current_units current_units,
	fa_additions_b.attribute_category_code asset_category,
	fa_categories_tl.description asset_category_desc,
	fa_books.depreciate_flag depreciate_flg,
	fa_categories_b.last_update_date cat_b_last_upd_dt,
	fa_categories_tl.last_update_date cat_tl_last_upd_dt,
	fa_books.last_update_date  books_last_upd_dt,
	fa_deprn_periods.period_name period_retired_name,
	fa_books.original_cost,
    --added columns
	fa_books.PRODUCTION_CAPACITY,
    fa_books.ADJUSTED_RATE,
	decode(fa_additions_b.ASSET_TYPE, 'CIP', fa_category_books.CIP_COST_ACCT,fa_category_books.ASSET_COST_ACCT) GL_ACCOUNT,
    decode(fa_additions_b.ASSET_TYPE, 'CIP', NULL,fa_category_books.DEPRN_RESERVE_ACCT) RESERVE_ACCOUNT,
	  'N' as is_deleted_flg,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                   AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    ||'-'|| nvl(fa_additions_b.asset_id,0)
	||'-'||nvl(fa_books.book_type_code,'NA')
    ||'-'||nvl(fa_deprn_periods.period_name,'NA')	AS dw_load_id,
    getdate() AS dw_insert_date,
    getdate() AS dw_update_date
from bec_ods.fa_additions_b fa_additions_b 
left join bec_ods.fa_additions_tl fa_additions_tl 
on 
	fa_additions_b.asset_id = fa_additions_tl.asset_id 
	and fa_additions_tl.language = 'US'
inner join bec_ods.fa_categories_b fa_categories_b 
on 
	fa_categories_b.category_id = fa_additions_b.asset_category_id 
inner join bec_ods.fa_books fa_books 
on 
	fa_books.asset_id = fa_additions_b.asset_id
AND fa_books.date_ineffective IS NULL --added to consider only active rows	
inner join bec_ods.fa_categories_tl fa_categories_tl 
on 
	fa_categories_tl.category_id = fa_additions_b.asset_category_id 
	and fa_categories_tl.language = 'US'
left join bec_ods.fa_deprn_periods fa_deprn_periods 
on 
	fa_deprn_periods.period_counter = fa_books.period_counter_fully_retired
	and fa_deprn_periods.book_type_code = fa_books.book_type_code
--added tables
inner join bec_ods.fa_category_books fa_category_books
on  fa_category_books.book_type_code = fa_books.book_type_code 
AND fa_category_books.CATEGORY_ID = fa_additions_b.asset_category_id
	where
		(
		fa_additions_b.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_asset'
				and batch_name = 'fa') 
             
			or fa_books.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_asset'
				and batch_name = 'fa')
						or fa_deprn_periods.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_asset'
				and batch_name = 'fa')
				 )
 );
 commit;
-- Soft delete


update
	bec_dwh.DIM_FA_ASSET
set
	is_deleted_flg = 'Y'
where
	(nvl(asset_id, 0),
	nvl(book_type_code, 'NA'),
	nvl(period_retired_name,'NA')) not in (
	select
		nvl(ods.asset_id, 0) as asset_id,
		nvl(ods.book_type_code, 'NA') as book_type_code,
		nvl(ods.period_name,'NA') as period_retired_name
	from
		bec_dwh.DIM_FA_ASSET dw,
		(
select 
	fa_additions_b.asset_id,
	fa_books.book_type_code,
	fa_deprn_periods.period_name
from (select * from bec_ods.fa_additions_b 
				where is_deleted_flg <> 'Y') fa_additions_b 
left join (select * from bec_ods.fa_additions_tl 
				where is_deleted_flg <> 'Y') fa_additions_tl 
on 
	fa_additions_b.asset_id = fa_additions_tl.asset_id 
	and fa_additions_tl.language = 'US'
inner join (select * from bec_ods.fa_categories_b 
				where is_deleted_flg <> 'Y') fa_categories_b 
on 
	fa_categories_b.category_id = fa_additions_b.asset_category_id 
inner join (select * from bec_ods.fa_books 
				where is_deleted_flg <> 'Y') fa_books 
on 
	fa_books.asset_id = fa_additions_b.asset_id
AND fa_books.date_ineffective IS NULL --added to consider only active rows	
inner join (select * from bec_ods.fa_categories_tl 
				where is_deleted_flg <> 'Y') fa_categories_tl 
on 
	fa_categories_tl.category_id = fa_additions_b.asset_category_id 
	and fa_categories_tl.language = 'US'
left join (select * from bec_ods.fa_deprn_periods 
				where is_deleted_flg <> 'Y') fa_deprn_periods 
on 
	fa_deprn_periods.period_counter = fa_books.period_counter_fully_retired
	and fa_deprn_periods.book_type_code = fa_books.book_type_code
--added tables
inner join (select * from bec_ods.fa_category_books 
				where is_deleted_flg <> 'Y') fa_category_books
on  fa_category_books.book_type_code = fa_books.book_type_code 
AND fa_category_books.CATEGORY_ID = fa_additions_b.asset_category_id		
) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(ods.asset_id, 0)|| '-' || nvl(ods.book_type_code, 'NA')
			||'-'||nvl(ods.period_name,'NA')
			);

commit;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_fa_asset'
	and batch_name = 'fa';

commit;