/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Facts.
# File Version: KPI v1.0
*/
begin;

Truncate table bec_dwh.FACT_RAPA_STG8;

Insert Into bec_dwh.FACT_RAPA_STG8
(
select
	inventory_item_id,
	organization_id,
	cost_type,
	plan_name,
	data_type,
	order_group,
	order_type_text,
	requisition_number,
	need_by_date,
	aging_period,
	dock_promised_date,
	part_number,
	description,
	planning_make_buy_code,
	NULL as category_name,
	pr_open_qty,
	Primary_quantity,
	unit_price,
	primary_unit_of_measure,
	extended_cost,
	po_line_type,
	vendor_name,
	std_cost,
	ext_std_cost,
	variance,
	source_organization_id,
	planner_code,
	buyer_name,
	transactional_uom_code,
	release_num,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || inventory_item_id as inventory_item_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || organization_id as organization_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || source_organization_id as source_organization_id_KEY,
	-- audit columns
	'N' as is_deleted_flg,
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
		source_system = 'EBS')|| '-' || nvl(inventory_item_id, 0)|| '-' || nvl(requisition_number, 'NA')|| '-' ||nvl(cost_type, 'NA')|| '-' || nvl(need_by_date, '1900-01-01 12:00:00')|| '-' || nvl(pr_open_qty, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
(
SELECT item_id as inventory_item_id,
	destination_organization_id as organization_id,
	 cost_type,
	NULL plan_name,
	'Receipt Forecast' data_type,
	'BLR Internal' order_group,
	'BLR Internal Forecast' order_type_text,
	pr.requisition_number,
	trunc(pr.need_by_date) need_by_date,
	0 aging_period,
	nvl(trunc(pr.schedule_ship_date), trunc(pr.need_by_date - nvl(pr.intransit_time, 0)::int)) dock_promised_date,
	pr.part_number,
	pr.description,
	pr.planning_make_buy_code,
	pr_open_qty,
	NULL::numeric(38, 10) as primary_quantity,
	pr.unit_price,
	pr.primary_unit_of_measure,
	(pr_open_qty * pr.unit_price) extended_cost ,
	NULL po_line_type,
	'BLR INTERNAL' vendor_name,
	item_cost std_cost,
	item_cost * pr_open_qty ext_std_cost,
	( pr_open_qty * pr.unit_price - (item_cost) * pr_open_qty ) variance,
	NULL::Numeric(15,0) source_organization_id,
	planner_code,
	buyer_name,
	pr.unit_meas_lookup_code transactional_uom_code,
	NULL::Numeric(15,0) release_num
FROM
	(
	SELECT decode(cost_type_id, 1, 'Frozen', 3 , 'Pending') cost_type,
		oola.ordered_quantity ord_quantity,
		oola.shipped_quantity so_ship_qty,
		oola.cancelled_quantity so_cancelled_qty,
		prl.destination_organization_id,
		prl.item_id,
		prl.deliver_to_location_id,
		prh.creation_date,
		prl.need_by_date,
		prh.segment1
                || '-'
                || (
			ooha.order_number
                ) requisition_number,
		prl.requisition_header_id,
		prl.requisition_line_id,
		prl.quantity pr_quantity,
		prl.unit_price,
		msi.primary_unit_of_measure,
		( oola.ordered_quantity - nvl(oola.shipped_quantity, 0) - nvl(oola.cancelled_quantity, 0) ) pr_open_qty,
		NULL,
		msi.segment1 part_number,
		msi.description,
		msi.planner_code,
		decode(msi.planning_make_buy_code, 1, 'Make', 'Buy') planning_make_buy_code,
		oola.ordered_quantity,
		oola.cancelled_flag,
		oola.cancelled_quantity,
		oola.promise_date,
		oola.schedule_ship_date,
		oola.flow_status_code, cic.item_cost ,
        (
		SELECT
			agent_name
		FROM
			(select * from bec_ods.po_agents_v where is_deleted_flg <> 'Y') poa
		WHERE
			poa.agent_id = nvl(prl.suggested_buyer_id, msi.buyer_id)
                ) buyer_name,
		prl.unit_meas_lookup_code,
		(SELECT
                MAX(intransit_time)
            FROM
                (select * from bec_ods.mtl_interorg_ship_methods where is_deleted_flg <> 'Y')
            WHERE
                    from_location_id = 144
                AND to_location_id = prl.deliver_to_location_id) as intransit_time		
	FROM
		(select * from bec_ods.po_requisition_headers_all where is_deleted_flg <> 'Y') prh,
		(select * from bec_ods.po_requisition_lines_all where is_deleted_flg <> 'Y') prl,
		(select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi,
		(select * from bec_ods.oe_order_lines_all where is_deleted_flg <> 'Y') oola,
		(select * from bec_ods.oe_order_headers_all where is_deleted_flg <> 'Y') ooha,
		(select * from bec_ods.cst_item_costs where is_deleted_flg <> 'Y')     cic
	WHERE
		prh.requisition_header_id = prl.requisition_header_id
		AND prh.type_lookup_code = 'INTERNAL'
		AND prl.item_id = msi.inventory_item_id
		AND prl.destination_organization_id = msi.organization_id
		AND prl.requisition_line_id = oola.source_document_line_id (+)
		AND nvl(prl.cancel_flag, 'N') = 'N'
		AND nvl(oola.cancelled_flag, 'N') = 'N'
		AND oola.cancelled_quantity = 0
		AND oola.flow_status_code <> 'CLOSED'
		AND ( oola.ordered_quantity - nvl(oola.shipped_quantity, 0) - nvl(oola.cancelled_quantity, 0) ) > 0
		AND oola.header_id  = ooha.header_id(+)
		AND prl.destination_organization_id = cic.organization_id(+)
		AND prl.item_id  = cic.inventory_item_id(+) 
		AND cost_type_id in (1, 3)
        ) pr
)
);

commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_rapa_stg8'
	and batch_name = 'ascp';

commit;