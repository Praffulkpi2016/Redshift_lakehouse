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

drop table if exists bec_dwh.DIM_ITEM_COST;

create table bec_dwh.DIM_ITEM_COST 
DISTKEY (COST_TYPE_ID)
SORTKEY (LAST_UPDATE_DATE)
as
(select
	INVENTORY_ITEM_ID
,ORGANIZATION_ID
,COST_TYPE_ID
,OVERHEAD_COST
,OUTSIDE_PROCESSING_COST
,MATERIAL_OVERHEAD_COST
,MATERIAL_COST
,LOT_SIZE
,UNBURDENED_COST
,TL_RESOURCE
,TL_OVERHEAD
,TL_OUTSIDE_PROCESSING
,TL_MATERIAL_OVERHEAD
,TL_MATERIAL
,TL_ITEM_COST
,SHRINKAGE_RATE
,ROLLUP_ID
,RESOURCE_COST
,REQUEST_ID
,PROGRAM_UPDATE_DATE
,PROGRAM_ID
,PROGRAM_APPLICATION_ID
,PL_RESOURCE
,PL_OVERHEAD
,PL_OUTSIDE_PROCESSING
,PL_MATERIAL_OVERHEAD
,PL_MATERIAL
,PL_ITEM_COST
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,LAST_UPDATE_DATE
,ITEM_COST
,INVENTORY_ASSET_FLAG
,DEFAULTED_FLAG
,CREATION_DATE
,CREATED_BY
,COST_UPDATE_ID
,BURDEN_COST
,BASED_ON_ROLLUP_FLAG
,ASSIGNMENT_SET_ID
	-- audit columns
	,'N' as is_deleted_flg,
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
		source_system = 'EBS')
	|| '-' || nvl(INVENTORY_ITEM_ID, 0)
	|| '-' || nvl(ORGANIZATION_ID, 0) 
	|| '-' || nvl(COST_TYPE_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
	from bec_ods.CST_ITEM_COSTS
);
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_item_cost'
	and batch_name = 'costing';

commit;