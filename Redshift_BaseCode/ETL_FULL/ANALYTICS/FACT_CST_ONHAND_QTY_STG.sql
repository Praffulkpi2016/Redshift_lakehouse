/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for TEMPORARY STAGING TABLE.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh.FACT_CST_ONHAND_QTY_STG;

create table bec_dwh.FACT_CST_ONHAND_QTY_STG 
	diststyle all
	sortkey (transaction_id,inventory_item_id,organization_id)
as
(
select
	msi.segment1,
	msi.description,
	mmt.transaction_date,
	mmt.transaction_action_id,
	mmt.logical_transaction,
	msi.primary_unit_of_measure,
	msi.inventory_item_status_code,
	msi.planning_make_buy_code,
	msi.mrp_planning_code,
	mmt.revision,
	mmt.transaction_type_id,
	mmt.transaction_source_type_id,
	mmt.transaction_source_id,
	mmt.subinventory_code,
	mmt.transfer_subinventory,
	msi.primary_uom_code,
	msi.serial_number_control_code,
	msi.lot_control_code,
	decode(
      mmt.owning_organization_id, mmt.organization_id, 
      'N', 'Y'
    ) vmi_flag,
	mmt.new_cost,
	mmt.transaction_id,
	mmt.transaction_set_id,
	mmt.created_by,
	mmt.transaction_reference,
	mil1.segment1 || '.' || mil1.segment2 || '.' || mil1.segment3 locator,
	mmt.locator_id,
	mmt.inventory_item_id,
	mmt.organization_id,
	mmt.rcv_transaction_id,
	mmt.move_order_line_id,
	mmt.move_transaction_id,
	mmt.rma_line_id,
	mmt.operation_seq_num,
	mmt.distribution_account_id,
	mmt.shipment_number,
	mmt.last_update_date,
	mmt.primary_quantity as mmt_primary_quantity,
	mc.segment1 cat_seg1,
	mc.segment2 cat_seg2,
	(mc.segment1 || '.' || mc.segment2) category,
	ood.organization_name,
	ood.operating_unit operating_unit,
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
		source_system = 'EBS')|| '-' || nvl(mmt.inventory_item_id, 0)|| '-' || nvl(mmt.organization_id, 0)
	|| '-' || nvl(mmt.transaction_id, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	(select * from bec_ods.mtl_material_transactions where is_deleted_flg <> 'Y') mmt,
	(select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi,
	(select * from bec_ods.mtl_item_locations where is_deleted_flg <> 'Y') mil1,
	(select * from bec_ods.mtl_item_categories where is_deleted_flg <> 'Y') mic,
	(select * from bec_ods.mtl_categories_b where is_deleted_flg <> 'Y') mc,
	(select * from bec_ods.org_organization_definitions where is_deleted_flg <> 'Y') ood
where
	msi.inventory_item_id = mic.inventory_item_id (+)
	and msi.organization_id = mic.organization_id (+)
	and mic.category_id = mc.category_id (+)
	and mic.category_set_id (+) = 1
	and msi.organization_id = ood.organization_id
	and nvl(
      ood.disable_date, 
      (GETDATE() + 999)
    ) > GETDATE()
	and mmt.inventory_item_id = msi.inventory_item_id (+)
	and mmt.organization_id = msi.organization_id (+)
	and mmt.locator_id = mil1.inventory_location_id (+)
	and mmt.organization_id = mil1.organization_id (+)
);

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_cst_onhand_qty_stg'
	and batch_name = 'costing';

commit;