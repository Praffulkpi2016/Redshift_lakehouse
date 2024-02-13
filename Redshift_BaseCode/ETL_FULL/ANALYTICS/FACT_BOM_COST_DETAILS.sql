/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for FACTS.
# File Version: KPI v1.0
*/


begin;

drop table if exists bec_dwh.FACT_BOM_COST_DETAILS;

create table bec_dwh.FACT_BOM_COST_DETAILS 
diststyle all 
sortkey(component_item_id)
as
select 
top_item_id
,assembly_item_id
,component_item_id
,organization_id
,sort_order
,top_assembly
,assembly
,component
,bom_level
,description
,item_type
,parent_is_make_buy
,make_buy
,status
,item_num
,operation_seq_num
,primary_uom_code
,component_quantity
,extended_quantity
,costed_flag
,wip_supply_type
,bom_type
,'N' as is_deleted_flg,
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
		source_system = 'EBS')
|| '-' || nvl(component_item_id, 0)
|| '-' || nvl(organization_id, 0)
|| '-' || nvl(assembly_item_id, 0)
|| '-' || nvl(sort_order, 'NA')
	as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
(
SELECT 
  xib.top_item_id ,
  bom.assembly_item_id,
  xib.component_item_id,
  xib.organization_id,
trim('.' from
decode(substring(sort_order,1,7),null,'',trim(0 from substring(sort_order,1,7)))
||decode(substring(sort_order,8,7),null,'','.' ||trim(0 from substring(sort_order,8,7)))
||decode(substring(sort_order,15,7),null,'','.' ||trim(0 from substring(sort_order,15,7)))
||decode(substring(sort_order,22,7),null,'','.' ||trim(0 from substring(sort_order,22,7)))
||decode(substring(sort_order,29,7),null,'','.' ||trim(0 from substring(sort_order,29,7)))
||decode(substring(sort_order,36,7),null,'','.' ||trim(0 from substring(sort_order,36,7)))
||decode(substring(sort_order,43,7),null,'','.' ||trim(0 from substring(sort_order,43,7)))
||decode(substring(sort_order,50,7),null,'','.' ||trim(0 from substring(sort_order,50,7)))
||decode(substring(sort_order,57,7),null,'','.' ||trim(0 from substring(sort_order,57,7)))
||decode(substring(sort_order,64,7),null,'','.' ||trim(0 from substring(sort_order,64,7)))
||decode(substring(sort_order,71,7),null,'','.' ||trim(0 from substring(sort_order,71,7)))
||decode(substring(sort_order,78,7),null,'','.' ||trim(0 from substring(sort_order,78,7)))
||decode(substring(sort_order,85,7),null,'','.' ||trim(0 from substring(sort_order,85,7)))
||decode(substring(sort_order,92,7),null,'','.' ||trim(0 from substring(sort_order,92,7)))
||decode(substring(sort_order,99,7),null,'','.' ||trim(0 from substring(sort_order,99,7)))
||decode(substring(sort_order,106,7),null,'','.' ||trim(0 from substring(sort_order,106,7)))
||decode(substring(sort_order,113,7),null,'','.' ||trim(0 from substring(sort_order,113,7)))
||decode(substring(sort_order,120,7),null,'','.' ||trim(0 from substring(sort_order,120,7)))
||decode(substring(sort_order,127,7),null,'','.' ||trim(0 from substring(sort_order,127,7)))
) sort_order,
	msi2.segment1 top_assembly,
	msi3.segment1 assembly,
	msi.segment1 component,
	xib.plan_level bom_level,
	replace (replace (replace (replace (msi.description,	'&',	' AND '),	'<',	' '),	'>',	' '),	'Â°',	'DEG ')    DESCRIPTION,
	DECODE (msi.planning_make_buy_code,	1,	'Make',	2,	'Buy',	null)  item_type,
	DECODE (msi3.planning_make_buy_code,1,	'Parent Is Make',	2,	'Parent Is Buy',null) parent_is_make_buy,
	NVL (xib.attribute12,DECODE (msi.planning_make_buy_code,1,	'Make',	2,	'Buy',	null))  Make_buy,
    msi.inventory_item_status_code status,
	xib.item_num,
	xib.operation_seq_num,
	msi.primary_uom_code,
	xib.component_quantity,
	xib.extended_quantity,
	DECODE (xib.extend_cost_flag,1,	'Yes',	2,	'No') Costed_Flag,
	lu1.meaning wip_supply_type, 
	lu2.meaning bom_type
from
    (select * from bec_ods.xxbec_ibom_rc_temp where is_deleted_flg <> 'Y') xib,
    (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi,
    (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi2,
    (select * from bec_ods.bom_structures_b where is_deleted_flg <> 'Y')  bom,
    (select * from bec_ods.mtl_system_items_b where is_deleted_flg <> 'Y') msi3,
    (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y') lu1,
    (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y') lu2
WHERE 1=1
    and xib.bill_sequence_id = bom.bill_sequence_id
    and xib.organization_id = bom.organization_id
    and bom.assembly_item_id = msi3.inventory_item_id
    and bom.organization_id = msi3.organization_id
    and xib.component_item_id = msi.inventory_item_id(+)
    and xib.organization_id = msi.organization_id(+)
    and xib.top_item_id = msi2.inventory_item_id(+)
    and xib.organization_id = msi2.organization_id(+)
    and msi.wip_supply_type  = lu1.LOOKUP_CODE and lu1.LOOKUP_TYPE = 'WIP_SUPPLY'
and   msi.BOM_ITEM_TYPE    = lu2.LOOKUP_CODE 
and lu2.LOOKUP_TYPE = 'BOM_ITEM_TYPE'
);

end;



UPDATE bec_etl_ctrl.batch_dw_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    dw_table_name  = 'fact_bom_cost_details'
	and batch_name = 'costing';

commit;