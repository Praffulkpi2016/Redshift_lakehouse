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

drop table if exists bec_dwh.FACT_PHY_INV_TAG_LIST;

create table bec_dwh.FACT_PHY_INV_TAG_LIST diststyle all sortkey(TAG_ID) as 
select
	distinct
    mpi.physical_inventory_name,
	pit.tag_number,
	pit.revision,
	pit.lot_number,
	pit.serial_num serial_number,
	pit.subinventory,
	pit.tag_quantity quantity,
	pit.tag_uom uom,
	pit.inventory_item_id,
	pit.organization_id,
	decode(pit.void_flag,1,'Void',2,'Active') as void_flag,
	pit.locator_id,
	pit.counted_by_employee_id,
	pit.TAG_ID,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||pit.locator_id   locator_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||pit.organization_id   organization_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||pit.inventory_item_id   inventory_item_id_KEY,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||pit.TAG_ID TAG_ID_KEY,
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
	) || '-' || nvl(TAG_ID, 0) as dw_load_id,
	getdate() as dw_insert_date, 
	getdate() as dw_update_date 
from
	(select * from bec_ods.mtl_physical_inventory_tags where is_deleted_flg <> 'Y') pit,
	(select * from bec_ods.mtl_physical_inventories where is_deleted_flg <> 'Y') mpi
where
	1 = 1
	and pit.physical_inventory_id = mpi.physical_inventory_id(+)
order by
	mpi.physical_inventory_name,
	pit.tag_number;
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_phy_inv_tag_list' 
  and batch_name = 'inv';
commit;