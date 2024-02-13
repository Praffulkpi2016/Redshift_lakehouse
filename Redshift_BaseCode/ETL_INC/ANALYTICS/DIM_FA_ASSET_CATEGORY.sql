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
	bec_dwh.DIM_FA_ASSET_CATEGORY
where
	nvl(trunc(category_id), 0) in (
	select
		nvl(trunc(ods.category_id), 0) as category_id
	from
		bec_dwh.DIM_FA_ASSET_CATEGORY dw,
		(
		select
			fa_categories_b.category_id,
			fa_categories_b.last_update_date,
			fa_categories_b.kca_seq_id
		from
			bec_ods.fa_categories_b fa_categories_b
		inner join bec_ods.fa_categories_tl fa_categories_tl
on
			fa_categories_b.category_id = fa_categories_tl.category_id
		where
			fa_categories_tl.language = 'US'
			and (fa_categories_b.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_asset_category'
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
			source_system = 'EBS')|| '-' || nvl(trunc(ods.category_id), 0)	
);

commit;
-- Insert records

insert
	into
	bec_dwh.DIM_FA_ASSET_CATEGORY
(
capitalize_flag,
	category_id,
	category_description,
	category_type,
	creation_date,
	created_by,
	enabled_flag,
	last_update_date,
	last_updated_by,
	owned_leased,
	property_1245_1250_code,
	property_type_code,
	segment1,
	segment2,
	segment3,
	segment4,
	segment5,
	segment6,
	segment7,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(
	select
		fa_categories_b.capitalize_flag,
		fa_categories_b.category_id,
		fa_categories_tl.description category_description,
		fa_categories_b.category_type,
		fa_categories_b.creation_date,
		fa_categories_b.created_by,
		fa_categories_b.enabled_flag,
		fa_categories_b.last_update_date,
		fa_categories_b.last_updated_by,
		fa_categories_b.owned_leased,
		fa_categories_b.property_1245_1250_code,
		fa_categories_b.property_type_code,
		fa_categories_b.segment1,
		fa_categories_b.segment2,
		fa_categories_b.segment3,
		fa_categories_b.segment4,
		fa_categories_b.segment5,
		fa_categories_b.segment6,
		fa_categories_b.segment7,
		-- audit columns
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
    || '-' || nvl(trunc(fa_categories_b.category_id), 0) as dw_load_id,
		getdate() as dw_insert_date,
		getdate() as dw_update_date
	from
		bec_ods.fa_categories_b fa_categories_b
	inner join bec_ods.fa_categories_tl fa_categories_tl
on
		fa_categories_b.category_id = fa_categories_tl.category_id
	where
		fa_categories_tl.language = 'US'
		and (fa_categories_b.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_fa_asset_category'
				and batch_name = 'fa')
				 )		
	   );
-- Soft delete

update
	bec_dwh.DIM_FA_ASSET_CATEGORY
set
	is_deleted_flg = 'Y'
where
	nvl(trunc(category_id), 0) not in (
	select
		nvl(trunc(ods.category_id), 0) as category_id
	from
		bec_dwh.DIM_FA_ASSET_CATEGORY dw,
		(
		select
			fa_categories_b.category_id
		from
			(select * from bec_ods.fa_categories_b 
				where is_deleted_flg <> 'Y') fa_categories_b
		inner join (select * from bec_ods.fa_categories_tl 
				where is_deleted_flg <> 'Y') fa_categories_tl
on
			fa_categories_b.category_id = fa_categories_tl.category_id
		where
			fa_categories_tl.language = 'US'
) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')|| '-' || nvl(trunc(ods.category_id), 0)	
);

commit;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_fa_asset_category'
	and batch_name = 'fa';

commit;