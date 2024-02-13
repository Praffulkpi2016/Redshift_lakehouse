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

Truncate table bec_dwh.FACT_RAPA_STG7;

Insert Into bec_dwh.FACT_RAPA_STG7
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
	so_ship_qty,
	primary_quantity,
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
		source_system = 'EBS')|| '-' || nvl(inventory_item_id, 0)|| '-' || nvl(dock_promised_date, '1900-01-01 12:00:00')|| '-' || nvl(cost_type, 'NA')|| '-' || nvl(need_by_date, '1900-01-01 12:00:00')|| '-' || nvl(so_ship_qty, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
(
SELECT pr.inventory_item_id,
    pr.destination_organization_id organization_id,
	pr.cost_type,
	NULL plan_name,
	'Receipt Actual' data_type,
	'BLR Internal' order_group,
	'BLR Internal Actuals' order_type_text,
	pr.requisition_number,
	trunc(pr.need_by_date) need_by_date,
	0 aging_period,
	trunc(transaction_date) dock_promised_date,
	pr.part_number,
	pr.description,
	pr.planning_make_buy_code,
	so_ship_qty,
	NULL::numeric(38, 10) as primary_quantity,
	pr.unit_price,
	pr.primary_unit_of_measure,
	so_ship_qty * pr.unit_price extended_cost,
	NULL po_line_type,
	'BLR INTERNAL' vendor_name,
	ch.standard_cost std_cost,
	ch.standard_cost*so_ship_qty ext_std_cost,
	(
	SELECT
		sum(base_transaction_value)
	FROM
		(select * from bec_ods.mtl_transaction_accounts where is_deleted_flg <> 'Y') mta,
		(select * from bec_ods.gl_code_combinations_kfv where is_deleted_flg <> 'Y') gcc
	WHERE
		mta.reference_account = gcc.code_combination_id
		AND transaction_id = pr.transaction_id
		AND gcc.segment3 = '52103'
        ) variance,
	NULL::Numeric(15,0) source_organization_id,
	planner_code,
	buyer_name,
	pr.unit_meas_lookup_code transactional_uom_code,
	NULL::Numeric(15,0) release_num
FROM
	(
	SELECT 'Frozen' cost_type,
		oola.line_id,
		mmt.inventory_item_id,
		mmt.transaction_date,
		(
		SELECT
			MAX(cost_update_id)
		FROM
			(select * from bec_ods.cst_cost_history_v where is_deleted_flg <> 'Y') ch
		WHERE
			ch.inventory_item_id = mmt.inventory_item_id
			AND ch.last_update_date <= mmt.transaction_date
			AND ch.organization_id = prl.destination_organization_id
                    ) cost_update_id,
		mmt.transaction_id,
		abs(mmt.transaction_quantity) so_ship_qty,
		prl.destination_organization_id,
		prl.item_id,
		prl.deliver_to_location_id,
		prh.creation_date,
		prl.need_by_date,
		prh.segment1
                || '-'
                || (
		SELECT
			ooha.order_number
		FROM
			(select * from bec_ods.oe_order_headers_all where is_deleted_flg <> 'Y') ooha
		WHERE
			ooha.header_id = oola.header_id
                ) requisition_number,
		prl.requisition_header_id,
		prl.requisition_line_id,
		prl.quantity pr_quantity,
		prl.unit_price,
		msi.segment1 part_number,
		msi.description,
		msi.planner_code,
		decode(msi.planning_make_buy_code, 1, 'Make', 'Buy') planning_make_buy_code,
		oola.ordered_quantity,
		oola.cancelled_flag,
		oola.cancelled_quantity,
		oola.promise_date,
		oola.schedule_ship_date,
		(
		SELECT
			agent_name
		FROM
			(select * from bec_ods.po_agents_v where is_deleted_flg <> 'Y') poa
		WHERE
			poa.agent_id = nvl(prl.suggested_buyer_id, msi.buyer_id)
                ) buyer_name,
		prl.unit_meas_lookup_code,
		msi.primary_unit_of_measure
	FROM
		(select * from bec_ods.po_requisition_headers_all where is_deleted_flg <> 'Y') prh,
		(select * from bec_ods.po_requisition_lines_all where is_deleted_flg <> 'Y') prl,
		(select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi,
		(select * from bec_ods.oe_order_lines_all where is_deleted_flg <> 'Y') oola,
		(select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt
	WHERE 
		prh.requisition_header_id = prl.requisition_header_id
		AND prl.item_id = msi.inventory_item_id
		AND oola.line_id = mmt.source_line_id
		AND oola.inventory_item_id = mmt.inventory_item_id
		and mmt.transaction_type_id = 62
		AND mmt.organization_id = 101
		AND mmt.transaction_date > sysdate - 365
		AND prl.destination_organization_id = msi.organization_id
		AND prl.requisition_line_id = oola.source_document_line_id (+)
		AND prh.type_lookup_code = 'INTERNAL'
		AND nvl(prl.cancel_flag, 'N') = 'N'
			AND nvl(oola.cancelled_flag, 'N') = 'N'
					AND nvl(oola.shipped_quantity, 0) > 0
	UNION all
	SELECT 'Pending' cost_type,
		oola.line_id,
		mmt.inventory_item_id,
		mmt.transaction_date,
		(
		SELECT
			MAX(cost_update_id)
		FROM
			(select * from bec_ods.cst_cost_history_v where is_deleted_flg <> 'Y') ch
		WHERE
			ch.inventory_item_id = mmt.inventory_item_id
			AND ch.last_update_date <= mmt.transaction_date
			AND ch.organization_id = prl.destination_organization_id
                    ) cost_update_id,
		mmt.transaction_id,
		abs(mmt.transaction_quantity) so_ship_qty,
		prl.destination_organization_id,
		prl.item_id,
		prl.deliver_to_location_id,
		prh.creation_date,
		prl.need_by_date,
		prh.segment1
                || '-'
                || (
		SELECT
			ooha.order_number
		FROM
			(select * from bec_ods.oe_order_headers_all where is_deleted_flg <> 'Y') ooha
		WHERE
			ooha.header_id = oola.header_id
                ) requisition_number,
		prl.requisition_header_id,
		prl.requisition_line_id,
		prl.quantity pr_quantity,
		prl.unit_price,
		msi.segment1 part_number,
		msi.description,
		msi.planner_code,
		decode(msi.planning_make_buy_code, 1, 'Make', 'Buy') planning_make_buy_code,
		oola.ordered_quantity,
		oola.cancelled_flag,
		oola.cancelled_quantity,
		oola.promise_date,
		oola.schedule_ship_date,
		(
		SELECT
			agent_name
		FROM
			(select * from bec_ods.po_agents_v where is_deleted_flg <> 'Y') poa
		WHERE
			poa.agent_id = nvl(prl.suggested_buyer_id, msi.buyer_id)
                ) buyer_name,
		prl.unit_meas_lookup_code,
		msi.primary_unit_of_measure
	FROM
		(select * from bec_ods.po_requisition_headers_all where is_deleted_flg <> 'Y') prh,
		(select * from bec_ods.po_requisition_lines_all where is_deleted_flg <> 'Y') prl,
		(select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi,
		(select * from bec_ods.oe_order_lines_all where is_deleted_flg <> 'Y') oola,
		(select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt
	WHERE 
		prh.requisition_header_id = prl.requisition_header_id
		AND prl.item_id = msi.inventory_item_id
		AND oola.line_id = mmt.source_line_id
		AND oola.inventory_item_id = mmt.inventory_item_id
		and mmt.transaction_type_id = 62
		AND mmt.organization_id = 101
		AND mmt.transaction_date > sysdate - 365
		AND prl.destination_organization_id = msi.organization_id
		AND prl.requisition_line_id = oola.source_document_line_id (+)
		AND prh.type_lookup_code = 'INTERNAL'
		AND nvl(prl.cancel_flag, 'N') = 'N'
			AND nvl(oola.cancelled_flag, 'N') = 'N'
					AND nvl(oola.shipped_quantity, 0) > 0
        ) pr,
        (select * from bec_ods.cst_cost_history_v where is_deleted_flg <> 'Y') ch
       WHERE pr.cost_update_id = ch.cost_update_id(+)
       and pr.inventory_item_id = ch.inventory_item_id(+)
       and pr.destination_organization_id = ch.organization_id(+)
       and pr.transaction_date >= ch.last_update_date(+)
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
	dw_table_name = 'fact_rapa_stg7'
	and batch_name = 'ascp';

commit;