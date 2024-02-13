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

drop table if exists bec_dwh.FACT_ASCP_ASSEMBLY_COMPS;

create table bec_dwh.FACT_ASCP_ASSEMBLY_COMPS diststyle all sortkey (assembly_item)
as
( 
WITH assembly_items
AS (
	SELECT DISTINCT msi.inventory_item_Id
		,msi.organization_id
	FROM bec_ods.mrp_forecast_dates mfd
		,bec_ods.msc_system_items msi
	WHERE 1 = 1
		AND forecast_date > sysdate
		AND current_forecast_quantity > 0
		AND mfd.organization_id = msi.organization_id
		--265
		AND forecast_designator LIKE '%.%'
		AND mfd.inventory_item_Id = msi.sr_inventory_item_id
		AND msi.plan_id = 40029
		AND msi.planning_make_buy_code = 1
		--and msi.inventory_item_Id = 2210003
	)
	,mov_forecast_qty
AS (
	SELECT using_assembly_item_id
		,inventory_item_id
		,organization_id
		,trunc(new_due_date) new_due_date
		,sum(quantity_rate) forecast_qty
	FROM bec_ods.msc_orders_v
	WHERE order_type_text = 'Forecast'
		AND plan_id = 40029
		AND category_id = 3411
		AND using_assembly_item_id = inventory_item_id
		AND planning_make_buy_code = 1
		AND quantity_rate < 0
	GROUP BY using_assembly_item_id
		,inventory_item_id
		,organization_id
		,trunc(new_due_date)
	)
SELECT msia.item_name assembly_item
	,msia.description assembly_item_desc
	,msia.sr_inventory_item_id assembly_sr_mtl_item_id
	,i.using_assembly_item_id Assembly_item_id
	,msic.item_name component_item
	,msic.description component_item_desc
	,msic.sr_inventory_item_id component_sr_mtl_item_id
	,msic.buyer_name component_buyer_name
	,msic.planner_code component_planner_code
	,i.inventory_item_id component_item_id
	,i.organization_id
	,i.quantity_per_assembly comp_qty_per_assembly
FROM (
	SELECT *
		,row_number() OVER (	
			PARTITION BY organization_id
			,using_assembly_item_id
			,inventory_item_id ORDER BY organization_id
				,using_assembly_item_id
				,inventory_item_id
				,assembly_count DESC
			) rownumber
	FROM (
		SELECT using_assembly_item_id
			,inventory_item_id
			,organization_id
			,quantity_per_assembly
			,count(quantity_per_assembly) assembly_count
		FROM (
			SELECT mov.using_assembly_item_id
				,mov.inventory_item_id
				,mov.organization_id
				,trunc(mov.new_due_date) new_due_date
				,sum(mov.quantity_rate) mov_plan_quantity
				,(
					SELECT sum(fc.forecast_qty)
					FROM mov_forecast_qty fc
					WHERE mov.using_assembly_item_id = fc.using_assembly_item_id
						AND mov.organization_id = fc.organization_id
						AND trunc(mov.new_due_date) = fc.new_due_date
					) total_forecast
				,sum(mov.quantity_rate) / (
					SELECT sum(fc.forecast_qty)
					FROM mov_forecast_qty fc
					WHERE mov.using_assembly_item_id = fc.using_assembly_item_id
						AND mov.organization_id = fc.organization_id
						AND trunc(mov.new_due_date) = fc.new_due_date
					) quantity_per_assembly
			FROM assembly_items items,
				bec_ods.msc_orders_v mov
			WHERE mov.order_type_text = 'Planned order demand'
				AND mov.plan_id = 40029
				AND mov.category_id = 3411
				--AND mov.organization_id = 265
				AND mov.using_assembly_item_id <> mov.inventory_item_id
				AND mov.planning_make_buy_code = 2
				AND trunc(mov.new_due_date) > trunc(sysdate)
				AND mov.quantity_rate < 0
				AND mov.using_assembly_item_id = items.inventory_item_Id
				AND mov.organization_id = items.organization_id
			GROUP BY mov.using_assembly_item_id
				,mov.new_due_date
				,mov.inventory_item_id
				,mov.organization_id
			)
		WHERE total_forecast IS NOT NULL
		GROUP BY using_assembly_item_id
			,inventory_item_id
			,organization_id
			,quantity_per_assembly
		)
	) i
	,bec_ods.msc_system_items msia
	,bec_ods.msc_system_items msic
WHERE rownumber = 1
	AND msia.inventory_item_id = i.using_assembly_item_id
	AND msia.organization_id = i.organization_id
	AND msia.plan_id = 40029
	AND msic.inventory_item_id = i.inventory_item_id
	AND msic.organization_id = i.organization_id
	AND msic.plan_id = 40029
ORDER BY i.organization_id
	,i.using_assembly_item_id
	,i.inventory_item_id
	,i.assembly_count DESC
);

commit;
end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ascp_assembly_comps'
	and batch_name = 'ascp';

commit;