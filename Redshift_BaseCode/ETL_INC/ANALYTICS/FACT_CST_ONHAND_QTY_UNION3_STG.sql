/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for TEMPORARY STAGING TABLE.
# File Version: KPI v1.0
*/
begin;

Truncate table bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG;

Insert into bec_dwh.FACT_CST_ONHAND_QTY_UNION3_STG 
(
select
	mmt.segment1,
	mmt.cat_seg1,
	mmt.cat_seg2,
	mmt.category,
	mmt.organization_name,
	mmt.operating_unit,
	mmt.description,
	mmt.primary_unit_of_measure,
	mmt.primary_uom_code,
	mmt.inventory_item_status_code,
	mmt.planning_make_buy_code,
	mmt.mrp_planning_code,
	mmt.inventory_item_id,
	mut.transaction_date,
	msn.initialization_date,
	mmt.organization_id,
	mmt.transaction_source_type_id,
	mmt.transaction_type_id,
	msn.lot_number,
	msn.serial_number,
	(   mil.segment1
                           || '.'
                           || mil.segment2
                           || '.'
                           || mil.segment3) locator,
	msn.current_subinventory_code,
	sub.material_account,
	sub.asset_inventory,
	sub.attribute10,
	sub.attribute11,
	mmt.vmi_flag,
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
		source_system = 'EBS')|| '-' || nvl(mmt.segment1, 'NA')|| '-' || nvl(mmt.organization_id, 0)
	|| '-' || nvl(mmt.locator, 'NA')|| '-' || nvl(msn.lot_number, 'NA')|| '-' || nvl(mut.transaction_date, '1900-01-01 12:00:00')|| '-' || nvl(msn.serial_number, 'NA') as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_dwh.FACT_CST_ONHAND_QTY_STG mmt,
	(select * from bec_ods.mtl_serial_numbers where is_deleted_flg <> 'Y') msn,
	(select * from bec_ods.mtl_unit_transactions where is_deleted_flg <> 'Y') mut,
	(select * from bec_ods.mtl_secondary_inventories where is_deleted_flg <> 'Y') sub,
    (select * from bec_ods.mtl_item_locations where is_deleted_flg <> 'Y') mil
where
	msn.current_organization_id = mmt.organization_id
	and msn.last_transaction_id is null
	and mut.transaction_source_id = msn.last_txn_source_id
	and mut.transaction_date = msn.completion_date
	and mut.transaction_id = mmt.transaction_id (+)
	and msn.serial_number = mut.serial_number (+)
	AND mil.inventory_location_id(+) = msn.current_locator_id
	and msn.current_status = 3
	and mmt.serial_number_control_code <> 1
	and msn.inventory_item_id = mmt.inventory_item_id
	and msn.current_subinventory_code = sub.secondary_inventory_name (+)
	and msn.current_organization_id = sub.organization_id (+))
;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_cst_onhand_qty_union3_stg'
	and batch_name = 'costing';

commit;