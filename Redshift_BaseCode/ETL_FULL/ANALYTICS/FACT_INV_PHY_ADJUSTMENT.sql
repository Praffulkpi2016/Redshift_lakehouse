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

drop table if exists bec_dwh.FACT_INV_PHY_ADJUSTMENT;

create table bec_dwh.FACT_INV_PHY_ADJUSTMENT diststyle all sortkey (ORGANIZATION_ID,INVENTORY_ITEM_ID,ITEM)
as 	(select
	mpi.physical_inventory_name,
	msi.segment1 item,
	msi.description item_desc, 
	mpa.revision rev,
	mpa.subinventory_name subinv,
	mpa.creation_date as adjustment_date,
	DECODE (loc.segment1 || '.' || loc.segment2 || '.' || loc.segment3,	'..',	null,	loc.segment1 || '.' || loc.segment2 || '.' || loc.segment3) locator,
	mpa.lot_number, 
	mpa.serial_number,
	mpa.system_quantity,
	NVL (mpa.count_quantity,0) as count_quantity,
	msi.primary_uom_code uom,
	mpa.adjustment_quantity,
	NVL (mpa.system_quantity * mpa.actual_cost,0) system_value,
	NVL (mpa.count_quantity * mpa.actual_cost,0) count_value,
	NVL (mpa.adjustment_quantity * mpa.actual_cost,0) adjustment_value,
	mpa.outermost_lpn_id,
	mpa.parent_lpn_id,
	mpa.cost_group_id, 
	mpa.approved_by_employee_id,
	msi.inventory_item_id,
	mpa.organization_id, 
	org.organization_name,
	NVL (mpa.count_quantity * mpa.actual_cost,	0)- NVL (system_quantity * mpa.actual_cost,	0) sort_by,
	on_hand.total_qoh perpetual_qty,
	(select
		cic.item_cost
	from
		(select * from bec_ods.cst_item_costs where is_deleted_flg<>'Y') cic
	where
		cic.cost_type_id in (
		select
			cct.cost_type_id
		from
			(select * from bec_ods.cst_cost_types where is_deleted_flg<>'Y') cct
		where
			cct.cost_type =
                                                           'Frozen')
		and organization_id = msi.organization_id
		and inventory_item_id = msi.inventory_item_id) item_cost,
on_hand.total_qoh
            * (
	select
		cic.item_cost
	from
		(select * from bec_ods.cst_item_costs where is_deleted_flg<>'Y') cic
	where
		cic.cost_type_id in (
		select
			cct.cost_type_id
		from
			(select * from bec_ods.cst_cost_types where is_deleted_flg<>'Y') cct
		where
			cct.cost_type = 'Frozen')
		and organization_id = msi.organization_id
		and inventory_item_id = msi.inventory_item_id) as perpetual_value,
	NVL (mpa.approval_status,1) item_approval_status,
	DECODE(NVL (mpa.approval_status,1),1,'Non-Rejected',3,'Non-Rejected',2, 'Rejected', Null) as  Include_Rej_Items, --Added for quickSight
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
			   || '-' || nvl(mpa.organization_id, 0)
			   || '-' || nvl(msi.inventory_item_id, 0) 
			   || '-' || nvl(mpi.physical_inventory_name, 'NA')
			   || '-' || nvl(mpa.subinventory_name, 'NA')
			   || '-' || nvl(locator, 'NA')
			   || '-' || nvl(mpa.serial_number, 'NA')
			   || '-' || nvl(mpa.lot_number, 'NA') 
			   as dw_load_id, 
			getdate() as dw_insert_date,
			getdate() as dw_update_date	
from
(select * from bec_ods.mtl_system_items_b where is_deleted_flg<>'Y') msi
inner join (select * from bec_ods.mtl_physical_adjustments where is_deleted_flg<>'Y') mpa
on msi.inventory_item_id = mpa.inventory_item_id
and msi.organization_id = mpa.organization_id
left outer join (select * from bec_ods.mtl_item_locations where is_deleted_flg<>'Y') loc
on mpa.locator_id = loc.inventory_location_id
and mpa.organization_id = loc.organization_id
left outer join (select * from bec_ods.mtl_physical_inventories where is_deleted_flg<>'Y') mpi
on mpa.organization_id = mpi.organization_id
and mpa.physical_inventory_id = mpi.physical_inventory_id
inner join (select * from bec_ods.org_organization_definitions where is_deleted_flg<>'Y') org
on mpa.organization_id = org.organization_id
left outer join 
	(select	sum(total_qoh) as total_qoh,
	locator_id,organization_id,
	inventory_item_id,subinventory_code
	from 	(select * from bec_ods.mtl_onhand_locator_v where is_deleted_flg<>'Y')mtl_onhand_locator_v
	group by locator_id,organization_id,
	inventory_item_id,subinventory_code)on_hand
	on (( on_hand.locator_id = loc.inventory_location_id
			or on_hand.locator_id is null)
		and on_hand.organization_id = mpa.organization_id
		and mpa.inventory_item_id = on_hand.inventory_item_id
		and on_hand.subinventory_code(+) = mpa.subinventory_name)
where mpa.adjustment_quantity <> 0
order by
	sort_by desc,
	subinv asc,
	item asc,
	rev asc,
	locator asc,
	lot_number asc,
	serial_number asc 
);

END;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_inv_phy_adjustment'
	and batch_name = 'inv';