/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for RT reports.
# File Version: KPI v1.0
*/
begin;

Truncate table bec_dwh_rpt.FACT_WO_NETTABLE_ONHAND_QTY_RT;

Insert Into bec_dwh_rpt.FACT_WO_NETTABLE_ONHAND_QTY_RT
(
with on_hand_qty as (
  select 
    sum(quantity) quantity, 
    part_number, 
    organization_id, 
    subinventory, 
    locator_id, 
    serial_number 
  from 
    bec_dwh.fact_inv_onhand moq1 
  where vmi_flag <> 'Y'
  group by 
    part_number, 
    organization_id, 
    subinventory, 
    locator_id, 
    serial_number
) 
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
  sum(moq1.quantity) on_hand_qty, 
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
	|| '-' || nvl(msi.secondary_inventory_name, 'NA')
	|| '-' || nvl(locator, 'NA')
	|| '-' || nvl(serial_number, 'NA')
   as dw_load_id -- WIP_ENTITY_ID,part_inventory_item_id,operation_seq_num,subinventory,locator,serial_number
from 
  bec_dwh.fact_wo_requirments wo, 
  bec_ods.wip_operation_resources wor, 
  on_hand_qty moq1, 
  bec_dwh.dim_sub_inventories msi, 
  bec_ods.MTL_MATERIAL_STATUSES_TL msi_status, 
  bec_dwh.dim_item_locations loc 
where 1=1
  --nvl(wo.date_completed,getdate()) >= '2022-01-01 00:00:00.000'
  and job_status_type != 'Closed'
  AND wo.wip_entity_id = wor.wip_entity_id 
  AND wo.organization_id = wor.organization_id 
  AND wo.operation_seq_num = wor.operation_seq_num 
  and wo.component = moq1.part_number(+) 
  and wo.organization_id = moq1.organization_id(+) 
  AND moq1.subinventory = msi.secondary_inventory_name(+) 
  AND moq1.organization_id = msi.organization_id (+) 
  and msi.status_id = msi_status.status_id(+) 
  and moq1.locator_id = loc.inventory_location_id(+)
group by 
wo.job_no, 
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
  wo.supply_type, 
  wo.organization_id, 
  wo.organization_code, 
  wo.ORGANIZATION_NAME, 
  --wo.organization_id, 
  wo.component, 
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
  ), 
  wo.quantity_per_assembly, 
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
  ), 
  wo.item_cost,
  wo.dw_load_id 
);
  
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