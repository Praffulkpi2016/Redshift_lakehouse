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

BEGIN;

delete from bec_dwh.FACT_INV_PHY_ADJUSTMENT
where (
	nvl(ORGANIZATION_ID,0),
	nvl(INVENTORY_ITEM_ID,0),
	nvl(physical_inventory_name, 'NA'),
	nvl(subinv, 'NA')	,	
    nvl(locator, 'NA')	,
	nvl(serial_number, 'NA'),
	nvl(lot_number, 'NA') 
) in 
(select 
	nvl(ods.ORGANIZATION_ID,0) as ORGANIZATION_ID,
	nvl(ods.INVENTORY_ITEM_ID,0) as INVENTORY_ITEM_ID,
	nvl(ods.physical_inventory_name, 'NA') as physical_inventory_name,
	nvl(ods.subinv, 'NA') as subinv,
    nvl(ods.locator, 'NA') as locator,		   
	nvl(ods.serial_number, 'NA') as serial_number,
	nvl(ods.lot_number, 'NA') as lot_number 
from bec_dwh.FACT_INV_PHY_ADJUSTMENT dw ,
	(select
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
	bec_ods.cst_item_costs   cic
	where
		cic.cost_type_id in (
		select
			cct.cost_type_id
		from
		bec_ods.cst_cost_types   cct
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
			bec_ods.cst_item_costs   cic
	where
		cic.cost_type_id in (
		select
			cct.cost_type_id
		from
		bec_ods.cst_cost_types   cct
		where
			cct.cost_type = 'Frozen')
		and organization_id = msi.organization_id
		and inventory_item_id = msi.inventory_item_id) as perpetual_value,
	NVL (mpa.approval_status,1) item_approval_status,
    DECODE(NVL (mpa.approval_status,1),1,'Non-Rejected',3,'Non-Rejected',2, 'Rejected', Null) as  Include_Rej_Items, --Added for quickSight

	
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
	bec_ods.mtl_system_items_b   msi
	inner join   bec_ods.mtl_physical_adjustments   mpa
	on msi.inventory_item_id = mpa.inventory_item_id
	and msi.organization_id = mpa.organization_id
	left outer join   bec_ods.mtl_item_locations   loc
	on mpa.locator_id = loc.inventory_location_id
	and mpa.organization_id = loc.organization_id
	left outer join   bec_ods.mtl_physical_inventories   mpi
	on mpa.organization_id = mpi.organization_id
	and mpa.physical_inventory_id = mpi.physical_inventory_id
	inner join   bec_ods.org_organization_definitions   org
	on mpa.organization_id = org.organization_id
	left outer join 
	(select	sum(total_qoh) as total_qoh,
	locator_id,organization_id,
	inventory_item_id,subinventory_code,is_deleted_flg
	from 	 bec_ods.mtl_onhand_locator_v  mtl_onhand_locator_v
	group by locator_id,organization_id,
	inventory_item_id,subinventory_code,is_deleted_flg)on_hand
	on (( on_hand.locator_id = loc.inventory_location_id
			or on_hand.locator_id is null)
		and on_hand.organization_id = mpa.organization_id
		and mpa.inventory_item_id = on_hand.inventory_item_id
		and on_hand.subinventory_code(+) = mpa.subinventory_name)
where mpa.adjustment_quantity <> 0
	AND (mpa.kca_seq_date > (
		SELECT
			(executebegints-prune_days)
		FROM
			bec_etl_ctrl.batch_dw_info
		WHERE
			dw_table_name = 'fact_inv_phy_adjustment'
			AND batch_name = 'inv')
			or 
			 msi.kca_seq_date > (
		SELECT
			(executebegints-prune_days)
		FROM
			bec_etl_ctrl.batch_dw_info
		WHERE
			dw_table_name = 'fact_inv_phy_adjustment'
			AND batch_name = 'inv')
		or 
		 mpi.kca_seq_date > (
		SELECT
			(executebegints-prune_days)
		FROM
			bec_etl_ctrl.batch_dw_info
		WHERE
			dw_table_name = 'fact_inv_phy_adjustment'
			AND batch_name = 'inv') 
			
			or mpa.is_deleted_flg = 'Y'
			or msi.is_deleted_flg = 'Y'
			or loc.is_deleted_flg = 'Y'
			or mpi.is_deleted_flg = 'Y'
			or org.is_deleted_flg = 'Y'
			or on_hand.is_deleted_flg = 'Y'
			  )
order by
	sort_by desc,
	subinv asc,
	item asc,
	rev asc,
	locator asc,
	lot_number asc,
	serial_number asc  ) ods
where  dw.dw_load_id = 
		(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')   
			   || '-' || nvl(ods.organization_id, 0)
			   || '-' || nvl(ods.inventory_item_id, 0) 
			   || '-' || nvl(ods.physical_inventory_name, 'NA')
			   || '-' || nvl(ods.subinv, 'NA')		
			   || '-' || nvl(ods.locator, 'NA')			   
			   || '-' || nvl(ods.serial_number, 'NA')
			   || '-' || nvl(ods.lot_number, 'NA') 
);
commit;

-- Insert Records 
INSERT INTO bec_dwh.FACT_INV_PHY_ADJUSTMENT
(
	physical_inventory_name,
	item,
	item_desc,
	rev,
	subinv,
	adjustment_date,
	"locator",
	lot_number,
	serial_number,
	system_quantity,
	count_quantity,
	uom,
	adjustment_quantity,
	system_value,
	count_value,
	adjustment_value,
	outermost_lpn_id,
	parent_lpn_id,
	cost_group_id,
	approved_by_employee_id,
	inventory_item_id,
	organization_id,
	organization_name,
	sort_by,
	perpetual_qty,
	item_cost,
	perpetual_value,
	item_approval_status,
	Include_Rej_Items,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
(select
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
 AND (mpa.kca_seq_date > (
		SELECT
			(executebegints-prune_days)
		FROM
			bec_etl_ctrl.batch_dw_info
		WHERE
			dw_table_name = 'fact_inv_phy_adjustment'
			AND batch_name = 'inv')
			or 
			 msi.kca_seq_date > (
		SELECT
			(executebegints-prune_days)
		FROM
			bec_etl_ctrl.batch_dw_info
		WHERE
			dw_table_name = 'fact_inv_phy_adjustment'
			AND batch_name = 'inv')
		or 
		 mpi.kca_seq_date > (
		SELECT
			(executebegints-prune_days)
		FROM
			bec_etl_ctrl.batch_dw_info
		WHERE
			dw_table_name = 'fact_inv_phy_adjustment'
			AND batch_name = 'inv'))
order by
	sort_by desc,
	subinv asc,
	item asc,
	rev asc,
	locator asc,
	lot_number asc,
	serial_number asc  ) ;

END;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_inv_phy_adjustment'
	and batch_name = 'inv';

commit;