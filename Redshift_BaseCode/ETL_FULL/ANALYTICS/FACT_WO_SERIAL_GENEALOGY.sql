/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Fact.
# File Version: KPI v1.0
*/
begin;
drop table if exists bec_dwh.FACT_WO_SERIAL_GENEALOGY;

CREATE TABLE  bec_dwh.FACT_WO_SERIAL_GENEALOGY 
	diststyle all sortkey(work_order)
as
(
SELECT
part_number,
inventory_item_id,
organization_id,
 (select system_id from bec_etl_ctrl.etlsourceappid 
where source_system='EBS')||'-'||organization_id as organization_id_key,
part_description,
serial_number,
transaction_id,
transaction_date,
transaction_type_id,
transaction_source_id,
transaction_type,
transaction_source_name,
wip_entity_id,
work_order,
'N' as is_deleted_flg,
 (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                   AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'|| nvl(inventory_item_id,0)
	|| '-'|| nvl(organization_id,0)
	|| '-'|| nvl(serial_number,'NA')
	|| '-'|| nvl(transaction_id,0)
	|| '-'|| nvl(wip_entity_id,0)
	   AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
from
(
select 
		msi.segment1 PART_NUMBER,
		msi.inventory_item_id,
		msi.organization_id,
		msi.description Part_Description, 
		mut.serial_number,
		mmt.transaction_id,		
		mmt.transaction_Date,
		mmt.transaction_type_id,
		mmt.transaction_source_id,		
		mtt.transaction_type_name Transaction_Type,
	    mmt.TRANSACTION_SOURCE_NAME,
		we.WIP_ENTITY_ID,		
		we.WIP_ENTITY_NAME WORK_ORDER
from    
		(SELECT * FROM bec_ods.mtl_material_transactions WHERE IS_DELETED_FLG <> 'Y') mmt, 
		(SELECT * FROM bec_ods.mtl_unit_transactions WHERE IS_DELETED_FLG <> 'Y') mut, 
		(SELECT * FROM bec_ods.mtl_system_items_b WHERE IS_DELETED_FLG <> 'Y') msi, 
		(SELECT * FROM bec_ods.mtl_transaction_types WHERE IS_DELETED_FLG <> 'Y') mtt,
		(SELECT * FROM bec_ods.wip_entities WHERE IS_DELETED_FLG <> 'Y') we
where 
		    mmt.transaction_id = mut.transaction_id
	and mmt.inventory_item_id = msi.inventory_item_id(+)
	and mmt.organization_Id = msi.organization_id(+)
	and mmt.transaction_type_id = mtt.transaction_type_id(+)
	and mmt.transaction_source_id = we.WIP_ENTITY_ID(+)
	and mmt.organization_id = we.organization_id(+)
	and mmt.transaction_type_id in (44)
union
select 
		msi.segment1 PART_NUMBER,
		msi.inventory_item_id,
		msi.organization_id,
		msi.description Part_Description, 
		mut.serial_number,
		mmt.transaction_id,		
		mmt.transaction_Date,
		mmt.transaction_type_id,
		mmt.transaction_source_id,		
		mtt.transaction_type_name Transaction_Type,
	    mmt.TRANSACTION_SOURCE_NAME,
		we.WIP_ENTITY_ID,		
		we.WIP_ENTITY_NAME WORK_ORDER
from 
		(SELECT * FROM bec_ods.mtl_material_transactions WHERE IS_DELETED_FLG <> 'Y') mmt, 
		(SELECT * FROM bec_ods.mtl_unit_transactions WHERE IS_DELETED_FLG <> 'Y') mut, 
		(SELECT * FROM bec_ods.mtl_system_items_b WHERE IS_DELETED_FLG <> 'Y') msi, 
		(SELECT * FROM bec_ods.mtl_transaction_types WHERE IS_DELETED_FLG <> 'Y') mtt,
		(SELECT * FROM bec_ods.wip_entities WHERE IS_DELETED_FLG <> 'Y') we
where 
		mmt.transaction_id = mut.transaction_id
	and mmt.inventory_item_id = msi.inventory_item_id(+)
	and mmt.organization_Id = msi.organization_id(+)
	and mmt.transaction_type_id = mtt.transaction_type_id(+)
	and mmt.transaction_source_id = we.WIP_ENTITY_ID(+)
	and mmt.organization_id = we.organization_id(+)
	and mmt.transaction_type_id in (35, 17 , 43, 44)
	and mmt.INVENTORY_ITEM_ID in (1279864, 1229865, 1280864, 2203875)
	and mmt.transaction_Source_id in (
	select
		mmt.transaction_source_id
	from
		bec_ods.mtl_material_transactions mmt,
		bec_ods.mtl_unit_transactions mut
	where
		mmt.transaction_id = mut.transaction_id
		and mmt.transaction_type_id in (35, 17 , 43, 44))
union
select 
		msi.segment1 PART_NUMBER,
		msi.inventory_item_id,
		msi.organization_id,
		msi.description Part_Description, 
		mut.serial_number,
		mmt.transaction_id,		
		mmt.transaction_Date,
		mmt.transaction_type_id,
		mmt.transaction_source_id,		
		mtt.transaction_type_name Transaction_Type,
	    mmt.TRANSACTION_SOURCE_NAME,
		we.WIP_ENTITY_ID,		
		we.WIP_ENTITY_NAME WORK_ORDER
from 
		(SELECT * FROM bec_ods.mtl_material_transactions WHERE IS_DELETED_FLG <> 'Y') mmt, 
		(SELECT * FROM bec_ods.mtl_unit_transactions WHERE IS_DELETED_FLG <> 'Y') mut, 
		(SELECT * FROM bec_ods.mtl_system_items_b WHERE IS_DELETED_FLG <> 'Y') msi, 
		(SELECT * FROM bec_ods.mtl_transaction_types WHERE IS_DELETED_FLG <> 'Y') mtt,
		(SELECT * FROM bec_ods.wip_entities WHERE IS_DELETED_FLG <> 'Y') we
where   
		mmt.transaction_id = mut.transaction_id
	and mmt.inventory_item_id = msi.inventory_item_id(+)
	and mmt.organization_Id = msi.organization_id(+)
	and mmt.transaction_type_id = mtt.transaction_type_id(+)
	and mmt.transaction_source_id = we.WIP_ENTITY_ID(+)
	and mmt.organization_id = we.organization_id(+)
	and mmt.transaction_type_id in (35, 17 , 43, 44)
	and mut.serial_number in (
	select
		mut.serial_number
	from
		bec_ods.mtl_material_transactions mmt,
		bec_ods.mtl_unit_transactions mut
	where
		mmt.transaction_id = mut.transaction_id
		and mmt.transaction_type_id in (35, 17 , 43, 44))
	and mmt.transaction_Source_id in (
	select
		mmt.transaction_source_id
	from
		bec_ods.mtl_material_transactions mmt,
		bec_ods.mtl_unit_transactions mut
	where
		mmt.transaction_id = mut.transaction_id
		and mmt.transaction_type_id in (35, 17 , 43, 44))
union
select 
		msi.segment1 PART_NUMBER,
		msi.inventory_item_id,
		msi.organization_id,
		msi.description Part_Description, 
		mut.serial_number,
		mmt.transaction_id,		
		mmt.transaction_Date,
		mmt.transaction_type_id,
		mmt.transaction_source_id,		
		mtt.transaction_type_name Transaction_Type,
	    mmt.TRANSACTION_SOURCE_NAME,
		we.WIP_ENTITY_ID,		
		we.WIP_ENTITY_NAME WORK_ORDER
from 
		(SELECT * FROM bec_ods.mtl_material_transactions WHERE IS_DELETED_FLG <> 'Y') mmt, 
		(SELECT * FROM bec_ods.mtl_unit_transactions WHERE IS_DELETED_FLG <> 'Y') mut, 
		(SELECT * FROM bec_ods.mtl_system_items_b WHERE IS_DELETED_FLG <> 'Y') msi, 
		(SELECT * FROM bec_ods.mtl_transaction_types WHERE IS_DELETED_FLG <> 'Y') mtt,
		(SELECT * FROM bec_ods.wip_entities WHERE IS_DELETED_FLG <> 'Y') we   
where
	mmt.transaction_id = mut.transaction_id
	and mmt.inventory_item_id = msi.inventory_item_id(+)
	and mmt.organization_Id = msi.organization_id(+)
	and mmt.transaction_type_id = mtt.transaction_type_id(+)
	and mmt.transaction_source_id = we.WIP_ENTITY_ID(+)
	and mmt.organization_id = we.organization_id(+)
	and mmt.transaction_type_id in (35, 17 , 43, 44)
	and mmt.TRANSACTION_SOURCE_ID in 
		(
	select
		mmt.transaction_Source_id
	from
		bec_ods.mtl_material_transactions mmt,
		bec_ods.mtl_unit_transactions mut, 
		bec_ods.mtl_system_items_b msi,
		bec_ods.mtl_transaction_types mtt,
		bec_ods.wip_entities we
	where
		mmt.transaction_id = mut.transaction_id
		and mmt.inventory_item_id = msi.inventory_item_id(+)
		and mmt.organization_Id = msi.organization_id(+)
		and mmt.transaction_type_id = mtt.transaction_type_id(+)
		and mmt.transaction_source_id = we.WIP_ENTITY_ID(+)
		and mmt.organization_id = we.organization_id(+)
		and mmt.transaction_type_id in (35, 17 , 43, 44)
			and we.WIP_ENTITY_NAME like 'PWM%'
			and mut.serial_number in 
							(
			select
				mut.serial_number
			from
				bec_ods.mtl_material_transactions mmt,
				bec_ods.mtl_unit_transactions mut
			where
				mmt.transaction_id = mut.transaction_id
				and mmt.transaction_type_id in (35, 17 , 43, 44)
					and mmt.transaction_Source_id in 
			    						(
					select
						mmt.transaction_source_id
					from
						bec_ods.mtl_material_transactions mmt,
						
bec_ods.mtl_unit_transactions mut
					where
						mmt.transaction_id = mut.transaction_id
						and mmt.transaction_type_id in (35, 17 , 43, 44))))
)
);

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'fact_wo_serial_genealogy'
	and batch_name = 'wip';

commit;