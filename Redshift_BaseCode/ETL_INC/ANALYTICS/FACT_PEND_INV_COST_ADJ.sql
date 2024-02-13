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
Truncate table bec_dwh.FACT_PEND_INV_COST_ADJ;

Insert Into bec_dwh.FACT_PEND_INV_COST_ADJ
(select  
inventory_item_id
,organization_id
,item
,description
,uom
,subinventory_code
,organization_code
,new_cost_element_id
,cost_type
,new_cost_type_id
,cost_element
,old_unit_cost
,new_unit_cost
,onhand
,'N' as is_deleted_flg
,
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
			   || '-' || nvl(inventory_item_id, 0)
			   || '-' || nvl(organization_id, 0) 
			   || '-' || nvl(subinventory_code, 'NA') 
			   || '-' || nvl(new_cost_element_id, 0) 
			   || '-'|| nvl(new_cost_type_id,0)
			   as dw_load_id, 
			getdate() as dw_insert_date,
			getdate() as dw_update_date
from ( select  
	     q.inventory_item_id,
	q.organization_id,
	m.segment1 as item,
	replace (m.description,
	'&',
	' AND ') description,
	m.primary_uom_code as uom,
		 q.subinventory_code,
	o.organization_code,
	new1.cost_element_id as new_cost_element_id,
	(
	select
		COST_TYPE
	from
		(select * from bec_ods.cst_cost_types where is_deleted_flg<>'Y') cct
	where
		cct.COST_TYPE_ID = new1.cost_type_id ) as cost_type,
	new1.cost_type_id as new_cost_type_id,
	decode(new1.cost_element_id,1,'Material',2,'Material Overhead',3,'Resource',4,'Outside Processing',5,'Overhead','Others') cost_element,
	NVL (old1.old_unit_cost,
	0) old_unit_cost,
	NVL (new1.new_unit_cost,
	0) new_unit_cost,
	SUM (q.transaction_quantity) onhand
from
	(select * from bec_ods.mtl_onhand_quantities_detail where is_deleted_flg<>'Y') q
	inner join (select * from bec_ods.mtl_system_items_b where is_deleted_flg<>'Y') m
	on q.inventory_item_id = m.inventory_item_id
	and q.organization_id = m.organization_id
	and q.owning_organization_id = m.organization_id
	inner join (select * from bec_ods.org_organization_definitions where is_deleted_flg<>'Y') o
	on q.organization_id = o.organization_id
	inner join (select * from bec_ods.mtl_secondary_inventories where is_deleted_flg<>'Y') s
	on s.secondary_inventory_name = q.subinventory_code
	and s.organization_id = q.organization_id
	and NVL (s.disable_date,(SYSDATE + 999)) > SYSDATE
	and s.asset_inventory = 1
	inner join(
	select
		inventory_item_id,
		organization_id,
		cost_element_id,
		cost_type_id,
		sum(ITEM_COST) new_unit_cost
	from
		(select * from bec_ods.CST_ITEM_COST_DETAILS where is_deleted_flg<>'Y')CST_ITEM_COST_DETAILS
	where
		1 = 1
	group by
			inventory_item_id,
			organization_id,
			cost_element_id,
			cost_type_id) new1
	on q.inventory_item_id = new1.inventory_item_id
	and q.organization_id = new1.organization_id
	left outer join	(
	select
		   inventory_item_id,
		organization_id,
		cost_element_id,
		cost_type_id,
		sum(ITEM_COST) old_unit_cost
	from
		(select * from bec_ods.CST_ITEM_COST_DETAILS where is_deleted_flg<>'Y')CST_ITEM_COST_DETAILS
	where
		1 = 1
		and cost_type_id = 1
	group by
		   inventory_item_id,
		   organization_id,
		   cost_element_id,
		   cost_type_id) old1
	on q.inventory_item_id = old1.inventory_item_id
	and q.organization_id = old1.organization_id
	and new1.cost_element_id = old1.cost_element_id
	--AND q.inventory_item_id =11004576
	--         and m.segment1 in ('115544','154606')
group by 
	q.inventory_item_id,
	q.organization_id,
	m.segment1,
	m.description,
	m.primary_uom_code,
	new1.cost_element_id,
	new1.cost_type_id,
	old1.old_unit_cost,
	new1.new_unit_cost,
	q.subinventory_code,
	o.organization_code        
)
);
END;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_pend_inv_cost_adj'
	and batch_name = 'costing';

commit;