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

-- TEMPORARY Table

Truncate table bec_dwh.wo_temp;

Insert Into bec_dwh.wo_temp
(select * from 
(
with on_hand_qty as (
  select 
    sum(quantity) quantity, 
    part_number, 
    organization_id, 
    subinventory, 
    locator_id, 
    serial_number,
    lot_number	
  from 
    bec_dwh.fact_inv_onhand moq1 
  where vmi_flag <> 'Y'
  group by 
    part_number, 
    organization_id, 
    subinventory, 
    locator_id, 
    serial_number,
	lot_number
)
select   sum(moq1.quantity) as on_hand_qty, 
  moq1.serial_number,
  msi.secondary_inventory_name subinventory,
  loc.segment1 || '.' || loc.segment2 || '.' || loc.segment3 locator,
    msi_status.status_code, 
  Decode (
    Substring(
      (msi_status.status_code), 
      Length (msi_status.status_code), 
      1
    ), 
    'e', 
    'Y', 
    Substring(
      (msi_status.status_code), 
      Length (msi_status.status_code), 
      1
    )
  ) nettable,
  moq1.part_number,
  moq1.organization_id,
  moq1.locator_id,
  moq1.lot_number
from   on_hand_qty moq1, 
  bec_dwh.dim_sub_inventories msi, 
  bec_ods.MTL_MATERIAL_STATUSES_TL msi_status, 
  bec_dwh.dim_item_locations loc
where 1=1
  AND moq1.subinventory = msi.secondary_inventory_name(+) 
  AND moq1.organization_id = msi.organization_id (+) 
  and msi.status_id = msi_status.status_id(+) 
  and moq1.locator_id = loc.inventory_location_id(+)
group by 
moq1.serial_number,
  msi.secondary_inventory_name,
  loc.segment1 || '.' || loc.segment2 || '.' || loc.segment3,
    msi_status.status_code, 
  Decode (
    Substring(
      (msi_status.status_code), 
      Length (msi_status.status_code), 
      1
    ), 
    'e', 
    'Y', 
    Substring(
      (msi_status.status_code), 
      Length (msi_status.status_code), 
      1
    )
  ) ,
  moq1.part_number,
  moq1.organization_id,
    moq1.locator_id,
  moq1.lot_number
)
);
commit;

--delete records
delete from bec_dwh_rpt.FACT_WO_NETTABLE_ONHAND_QTY_RT
where 
dw_load_id
in 
(
select ods.dw_load_id 
from bec_dwh_rpt.FACT_WO_NETTABLE_ONHAND_QTY_RT dw,
(
select 
  (
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
   )
	|| '-' || wo.dw_load_id
	|| '-' || nvl(temp.subinventory, 'NA')
	|| '-' || nvl(temp.locator, 'NA')
	|| '-' || nvl(temp.serial_number, 'NA')
	|| '-' || nvl(temp.lot_number,'NA')
	as dw_load_id
from 
bec_dwh.fact_wo_requirments wo,
bec_dwh.wo_temp temp
where 1=1
and wo.component = temp.part_number(+) 
  and wo.organization_id = temp.organization_id(+) 
   AND (wo.dw_update_date > (select (executebegints-prune_days) 
   from bec_etl_ctrl.batch_dw_info where dw_table_name = 'fact_wo_nettable_onhand_qty_rt' and batch_name = 'wip')))ods
   where dw.dw_load_id = ods.dw_load_id
   )
   ;
commit;

insert into bec_dwh_rpt.FACT_WO_NETTABLE_ONHAND_QTY_RT
(
select 
  wo.job_no job_no, 
  wo.WIP_ENTITY_ID, 
  wo.creation_date, 
  wo.description, 
  wo.primary_part_no, 
  wo.primary_part_description, 
  wo.job_type, 
  wo.class_code, 
  wo.scheduled_start_date, 
  wo.date_released, 
  wo.scheduled_completion_date, 
  wo.date_completed, 
  wo.start_quantity, 
  wo.QUANTITY_REMAINING, 
  wo.QUANTITY_COMPLETED, 
  wo.QUANTITY_SCRAPPED, 
  wo.qty_in_queue, 
  wo.qty_running, 
  wo.qty_waiting_to_move, 
  wo.qty_rejected, 
  wo.net_quantity, 
  wo.bom_revision, 
  wo.schedule_group_name, 
  wo.project_no, 
  wo.task_no, 
  wo.job_status_type, 
  wo.supply_type wip_supply_type_disp, 
  wo.organization_id, 
  wo.organization_code, 
  wo.ORGANIZATION_NAME, 
  --wo.organization_id, 
  wo.component part_no, 
  wo.comp_description, 
  wo.part_inventory_item_id, 
  wo.operation_seq_num, 
  wo.item_primary_uom_code, 
  wo.date_required, 
  wo.required_quantity, 
  wo.quantity_issued, 
  Decode (
    (
      wo.required_quantity - wo.quantity_issued
    ), 
    0, 
    NULL, 
    Decode (
      Sign (wo.required_quantity), 
      -1 * Sign (wo.quantity_issued), 
      (
        wo.required_quantity - wo.quantity_issued
      ), 
      Decode (
        Sign (
          Abs (wo.required_quantity) - Abs (wo.quantity_issued)
        ), 
        -1, 
        NULL, 
        (
          wo.required_quantity - wo.quantity_issued
        )
      )
    )
  ) quantity_open, 
  wo.quantity_per_assembly, 
  temp.on_hand_qty, 
  temp.serial_number, 
  temp.subinventory, 
  temp.locator, 
  temp.locator_id, 
  temp.lot_number,
  temp.status_code, 
temp.nettable, 
  wo.item_cost,
  (
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS'
   )
	|| '-' || wo.dw_load_id
	|| '-' || nvl(temp.subinventory, 'NA')
	|| '-' || nvl(temp.locator, 'NA')
	|| '-' || nvl(temp.serial_number, 'NA')
	|| '-' || nvl(temp.lot_number,'NA')
   as dw_load_id,
   'Y' ACTIVE_FLG
from 
bec_dwh.fact_wo_requirments wo,
bec_dwh.wo_temp temp
where 1=1
and nvl(wo.date_completed,getdate()) >= '2022-01-01 00:00:00.000'
and wo.component = temp.part_number(+) 
  and wo.organization_id = temp.organization_id(+) 
  AND (wo.dw_update_date > (select (executebegints-prune_days) 
   from bec_etl_ctrl.batch_dw_info where dw_table_name = 'fact_wo_nettable_onhand_qty_rt' and batch_name = 'wip'))
)  ;
 commit;
 
 -- Update ACTIVE Records

update bec_dwh_rpt.FACT_WO_NETTABLE_ONHAND_QTY_RT RT
set ACTIVE_FLG = 'N'
where not exists 
(
select distinct 
fio.part_number as part_no,
fio.organization_id,
fio.subinventory,
fio.locator_id,
fio.serial_number,
fio.lot_number
from 
bec_dwh.fact_inv_onhand fio
where 1=1
and fio.vmi_flag <> 'Y'
and nvl(RT.part_no,'NA') = nvl(fio.part_number,'NA')
and nvl(RT.organization_id,1) = nvl(fio.organization_id,1)
and nvl(RT.subinventory,'NA') = nvl(fio.subinventory,'NA')
and nvl(RT.locator_id,1)= nvl(fio.locator_id,1)
and nvl(RT.serial_number,'NA')= nvl(fio.serial_number,'NA')
and nvl(RT.lot_number,'NA')= nvl(fio.lot_number,'NA')
);
commit;
 
end;

update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_wo_nettable_onhand_qty_rt' 
  and batch_name = 'wip';
commit;
