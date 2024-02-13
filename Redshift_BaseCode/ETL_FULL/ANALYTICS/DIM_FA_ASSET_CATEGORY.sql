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

drop table if exists bec_dwh.DIM_FA_ASSET_CATEGORY;

create table bec_dwh.DIM_FA_ASSET_CATEGORY 
diststyle all 
sortkey(category_id)
as
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
);
end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_fa_asset_category' and batch_name = 'fa';

COMMIT;