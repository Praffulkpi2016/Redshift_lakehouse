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
-- Delete Records
delete from 
  bec_dwh.FACT_PHY_INV_TAG_LIST 
where 
  (
    nvl(TAG_ID, 0)
  ) in (
    select 
      nvl(ods.TAG_ID, 0) as TAG_ID
    from 
      bec_dwh.FACT_PHY_INV_TAG_LIST dw, 
      (
		select
			pit.TAG_ID 
		from
			bec_ods.mtl_physical_inventory_tags pit,
			bec_ods.mtl_physical_inventories mpi
		where
			1 = 1
			and pit.physical_inventory_id = mpi.physical_inventory_id(+)
			and (pit.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'fact_phy_inv_tag_list' and batch_name = 'inv')
			or pit.is_deleted_flg = 'Y'
			or mpi.is_deleted_flg = 'Y'
			)
	  ) ods 
    where 
      dw.dw_load_id = (
        select 
          system_id 
        from 
          bec_etl_ctrl.etlsourceappid 
        where 
          source_system = 'EBS'
      ) || '-' || nvl(ods.TAG_ID, 0)
  );
commit;
-- Insert records
insert into bec_dwh.FACT_PHY_INV_TAG_LIST (
  physical_inventory_name,
  tag_number,
  revision,
  lot_number,
  serial_number,
  subinventory,
  quantity,
  uom,
  inventory_item_id,
  organization_id,
  void_flag,
  locator_id,
  counted_by_employee_id,
  TAG_ID,
  locator_id_KEY,
  organization_id_KEY,
  inventory_item_id_KEY,
  TAG_ID_KEY,
  is_deleted_flg, 
  source_app_id, 
  dw_load_id, 
  dw_insert_date, 
  dw_update_date
) 
(
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
	and pit.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name = 'fact_phy_inv_tag_list' and batch_name = 'inv')
order by
	mpi.physical_inventory_name,
	pit.tag_number
);
end;
UPDATE 
  bec_etl_ctrl.batch_dw_info 
SET 
  load_type = 'I', 
  last_refresh_date = getdate() 
WHERE 
  dw_table_name = 'fact_phy_inv_tag_list' 
  and batch_name = 'inv';
COMMIT;