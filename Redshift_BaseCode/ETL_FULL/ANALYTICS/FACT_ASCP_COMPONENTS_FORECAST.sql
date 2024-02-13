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

drop table if exists bec_dwh.FACT_ASCP_COMPONENTS_FORECAST;

create table bec_dwh.fact_ascp_components_forecast diststyle all sortkey (assembly_item_id,organization_id)
as
( 
with forecast_data as (
select
	faf.organization_id,
	faf.part_number,
	faf.item_description ,
	faf.inventory_item_id,
	faf.buyer_name,
	faf.planner_code,
	faf.forecast_date ,
	faf.forecast_designator,
	PLANNING_MAKE_BUY_CODE,
	sum(faf.current_forecast_quantity) current_forecast_quantity
from
	bec_dwh.fact_ascp_forecast faf
where
	 faf.current_forecast_quantity > 0
group by
	faf.organization_id,
	faf.part_number,
	faf.item_description ,
	faf.inventory_item_id,
	faf.buyer_name,
	faf.planner_code,
	faf.forecast_date ,
	faf.forecast_designator,
	PLANNING_MAKE_BUY_CODE
)
select
	faf.organization_id,
	faf.part_number assembly_item,
	faf.item_description assembly_item_desc,
	faf.inventory_item_id assembly_item_id,
	faf.part_number component_item,
	faf.item_description component_item_desc,
	faf.inventory_item_id component_item_id,
	faf.buyer_name,
	faf.planner_code,
	faf.forecast_date ,
	faf.forecast_designator ,
	null component_qty   ,
	faf.current_forecast_quantity forecast_qty    ,
	faf.current_forecast_quantity component_forecast_qty
from
	forecast_data faf
where
	PLANNING_MAKE_BUY_CODE = 2
union 
select
	faf.organization_id,
	faf.part_number assembly_item,
	faf.item_description assembly_item_desc,
	faf.inventory_item_id assembly_item_id,
	comp.component_item,
	comp.component_item_desc,
	comp.component_sr_mtl_item_id component_item_id,
	comp.component_buyer_name buyer_name,
	comp.component_planner_code planner_code,
	faf.forecast_date ,
	faf.forecast_designator ,
	comp_qty_per_assembly component_qty,
	faf.current_forecast_quantity forecast_qty,
	(faf.current_forecast_quantity * comp_qty_per_assembly ) component_forecast_qty
from
	forecast_data faf,
	bec_dwh.fact_ascp_assembly_comps comp 
where
	faf.PLANNING_MAKE_BUY_CODE = 1
	and faf.inventory_item_id = comp.assembly_sr_mtl_item_id
	and faf.organization_id = comp.organization_id
);

commit;
end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ascp_components_forecast'
	and batch_name = 'ascp';

commit;