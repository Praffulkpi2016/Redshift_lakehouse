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

drop table if exists bec_dwh.FACT_ASCP_FORECAST;

create table bec_dwh.fact_ascp_forecast diststyle all sortkey (inventory_item_id,organization_id)
as
(
SELECT
    h.organization_id,
	h.inventory_item_id,
	l.transaction_id,
	h.concatenated_segments part_number,
	h.item_description,
	h.primary_uom_code,
	h.bom_item_type_desc,
	h.ato_forecast_control_desc,
	h.mrp_planning_code_desc,
	h.pick_components_flag,
	h.forecast_designator,
	l.bucket_type,
	l.forecast_date,
	l.current_forecast_quantity,
	l.original_forecast_quantity,
	l.comments,
	h.attribute1 HEADER_ATTRIBUTE1,
	l.attribute1 LINE_ATTRIBUTE1,
	msi.planner_code,
	msi.buyer_id,
	pa.agent_name buyer_name,
	msi.PLANNING_MAKE_BUY_CODE,
	getdate() AS  Datestamp,
		'N' AS IS_DELETED_FLG,
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
    ||'-'|| nvl(h.inventory_item_id, 0) 
	||'-'|| nvl(h.organization_id, 0)
	||'-'|| nvl(h.forecast_designator, 'NA')
	||'-'|| nvl(l.transaction_id, 0)	as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM
bec_ods.mrp_forecast_items_v h,
bec_ods.mrp_forecast_dates_v l,
bec_ods.mtl_system_items_b msi,
bec_ods.po_agents_v pa
WHERE
	h.organization_id = l.organization_id
	AND h.inventory_item_id = l.inventory_item_id
	AND h.forecast_designator = l.forecast_designator
	and l.inventory_item_id = msi.inventory_item_id(+)
	and l.organization_id = msi.organization_id(+)
	and msi.buyer_id = pa.agent_id(+)
);

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ascp_forecast'
	and batch_name = 'ascp';

commit;