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

truncate table bec_dwh.FACT_ASCP_HP_BOM_ITEMS;

insert into bec_dwh.fact_ascp_hp_bom_items
(
with recursive items(
  plan_id, organization_id, inventory_item_id, 
  using_assembly_id, level, root_assembly, 
  bill_sequence_id
) as (
  select 
    distinct bom1.plan_id, 
    bom1.organization_id, 
    bom1.assembly_item_id inventory_item_id, 
    bom1.assembly_item_id using_assembly_id, 
    0 as level, 
    bom1.assembly_item_id root_assembly, 
    bill_sequence_id 
  from 
   (select * from  bec_ods.msc_boms where is_deleted_flg <> 'Y')bom1, 
    (select * from  bec_ods.msc_system_items where is_deleted_flg <> 'Y') msi, 
    bec_dwh.fact_ascp_hp_items_stg fahp2 
  where 
    alternate_bom_designator#1 IS null and msi.inventory_item_id = bom1.assembly_item_id and msi.planning_make_buy_code = 1
    and msi.plan_id = bom1.plan_id 
    and msi.organization_id = bom1.organization_id 
    and fahp2.plan_id = bom1.plan_id 
    and fahp2.organization_id = bom1.organization_id 
    and fahp2.sr_instance_id = bom1.sr_instance_id 
  union all 
  select 
    mbc.plan_id, 
    mbc.organization_id, 
    mbc.inventory_item_id, 
    mbc.using_assembly_id, 
    level + 1 as level, 
    items.root_assembly, 
    nvl(bom2.bill_sequence_id, 1) 
  from 
    (select * from  bec_ods.msc_bom_components where is_deleted_flg <> 'Y') mbc, 
    (select * from  bec_ods.msc_boms where is_deleted_flg <> 'Y')bom2, 
    bec_dwh.fact_ascp_hp_items_stg fahp3, 
    items 
  where 
    sysdate BETWEEN mbc.effectivity_date 
    AND nvl(mbc.disable_date, sysdate) 
    AND mbc.effectivity_date <= sysdate 
    and mbc.plan_id = bom2.plan_id (+) 
    and alternate_bom_designator#1 IS null 
    and mbc.organization_id = bom2.organization_id (+) 
    and mbc.inventory_item_id = bom2.assembly_item_id (+) 
    and fahp3.plan_id = mbc.plan_id 
    and fahp3.organization_id = mbc.organization_id 
    and fahp3.sr_instance_id = mbc.sr_instance_id 
    and fahp3.inventory_item_id = mbc.inventory_item_id 
    and mbc.plan_id = items.plan_id 
    and mbc.organization_id = items.organization_id 
    and items.inventory_item_id = mbc.USING_ASSEMBLY_ID 
    and mbc.bill_sequence_id = nvl(items.bill_sequence_id, 1)
) 
select 
  *,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || inventory_item_id as inventory_item_id_key,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || organization_id as organization_id_key,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || using_assembly_id as using_assembly_id_key,
  (select system_id from bec_etl_ctrl.etlsourceappid where source_system = 'EBS')|| '-' || plan_id as plan_id_key,
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
		source_system = 'EBS')|| '-' || nvl(inventory_item_id, 0)|| '-' || nvl(using_assembly_id, 0)|| '-' || nvl(plan_id, 0)|| '-' || nvl(organization_id, 0) as dw_load_id, 
	getdate() as dw_insert_date,
	getdate() as dw_update_date 
from 
  items
);

commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_ascp_hp_bom_items'
	and batch_name = 'ascp';