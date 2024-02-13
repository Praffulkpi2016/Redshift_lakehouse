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

drop table if exists bec_dwh.FACT_WO_REQUIRMENTS;

create table bec_dwh.FACT_WO_REQUIRMENTS diststyle all
sortkey (WIP_ENTITY_ID) 
as
(       SELECT    
	       wdj.WIP_ENTITY_ID,
           wdj.ORGANIZATION_ID ,
           wdj.PRIMARY_ITEM_ID INVENTORY_ITEM_ID,
           we.wip_entity_name job_no,
           we.creation_date,
           we.description,
           msi1.segment1 AS primary_part_no,
           msi1.description AS primary_part_description,
           (SELECT meaning
              FROM bec_ods.fnd_lookup_values
             WHERE lookup_type = 'WIP_DISCRETE_JOB'
                   AND lookup_code = we.entity_type)
              AS job_type,
           wdj.class_code,
           wdj.scheduled_start_date,
           wdj.date_released,
           wdj.scheduled_completion_date,
           wdj.date_completed,
           wdj.start_quantity,
           DECODE (
                wdj.start_quantity
              - wdj.quantity_completed
              - wdj.quantity_scrapped,
              0, 0,
                wdj.start_quantity
              - wdj.quantity_completed
              - wdj.quantity_scrapped)
              "QUANTITY_REMAINING",
           DECODE (wdj.quantity_completed, 0, 0, wdj.quantity_completed)
              "QUANTITY_COMPLETED",
           DECODE (wdj.quantity_scrapped, 0, 0, wdj.quantity_scrapped)
              "QUANTITY_SCRAPPED",
           wo.operation_seq_num,
           wo.description operation_description,
           DECODE (wo.quantity_in_queue, 0, 0, wo.quantity_in_queue)
              qty_in_queue,
           DECODE (wo.quantity_running, 0, 0, wo.quantity_running)
              qty_running,
           DECODE (wo.quantity_waiting_to_move,
                   0, 0,
                   wo.quantity_waiting_to_move)
              qty_waiting_to_move,
           DECODE (wo.quantity_rejected, 0, 0, wo.quantity_rejected)
              qty_rejected,
           wdj.net_quantity,
           msi2.segment1 component,
           msi2.description comp_description,
           wro.required_quantity,
           wro.quantity_per_assembly,
           wro.quantity_issued,
           (SELECT meaning
              FROM bec_ods.fnd_lookup_values
             WHERE lookup_type = 'WIP_SUPPLY'
                   AND lookup_code = wro.wip_supply_type)
              supply_type,
           cic.item_cost,
           (NVL (wro.required_quantity, 0) * (cic.item_cost)) sched_amt,
           (NVL (wro.quantity_issued, 0) * (cic.item_cost)) actual_amt,
           (NVL (wro.required_quantity, 0) * (cic.item_cost))
           - (NVL (wro.quantity_issued, 0) * (cic.item_cost))
              VARIANCE,
           wdj.bom_revision,
           wsg.schedule_group_name,
           wdj.attribute1 project_no,
           wdj.attribute2 task_no,
           (SELECT meaning
              FROM bec_ods.fnd_lookup_values
             WHERE lookup_type = 'WIP_JOB_STATUS'
                   AND lookup_code = wdj.status_type)
              job_status_type,
           ood.ORGANIZATION_NAME,
           ood.ORGANIZATION_CODE,
		   wro.inventory_item_id as part_inventory_item_id,
			msi2.primary_uom_code item_primary_uom_code,
			wro.date_required,
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
	||'-'|| nvl(part_inventory_item_id, 0)
	||'-'||nvl(wdj.ORGANIZATION_ID,0)
	||'-'||nvl(wdj.WIP_ENTITY_ID,0)
	||'-'||nvl(wo.operation_seq_num,0)
	||'-'||nvl(JOB_NO,'NA')
	   as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
      FROM (SELECT * FROM bec_ods.wip_entities WHERE IS_DELETED_FLG <> 'Y') we,
           (SELECT * FROM bec_ods.wip_discrete_jobs WHERE IS_DELETED_FLG <> 'Y') wdj,
           (SELECT * FROM bec_ods.wip_operations WHERE IS_DELETED_FLG <> 'Y') wo,
           (SELECT * FROM bec_ods.wip_requirement_operations WHERE IS_DELETED_FLG <> 'Y') wro,
           (SELECT * FROM bec_ods.wip_schedule_groups WHERE IS_DELETED_FLG <> 'Y') wsg,
		   (SELECT * FROM bec_ods.mtl_system_items_b WHERE IS_DELETED_FLG <> 'Y') msi1,
           (SELECT * FROM bec_ods.mtl_system_items_b WHERE IS_DELETED_FLG <> 'Y')  msi2,
           (SELECT * FROM bec_ods.cst_item_costs WHERE IS_DELETED_FLG <> 'Y') cic,
           (SELECT * FROM bec_ods.org_organization_definitions WHERE IS_DELETED_FLG <> 'Y') ood
     WHERE     we.wip_entity_id = wdj.wip_entity_id
           AND we.organization_id = wdj.organization_id
           AND wdj.primary_item_id = msi1.inventory_item_id
           AND wdj.organization_id = msi1.organization_id
           AND wdj.schedule_group_id = wsg.schedule_group_id(+)
           AND wdj.wip_entity_id = wo.wip_entity_id
           AND wdj.organization_id = wo.organization_id
           AND wo.wip_entity_id = wro.wip_entity_id
           AND wo.operation_seq_num = wro.operation_seq_num
           AND wo.organization_id = wro.organization_id
           AND wro.inventory_item_id = msi2.inventory_item_id(+)
           AND wro.organization_id = msi2.organization_id(+)
           AND wro.inventory_item_id = cic.inventory_item_id(+)
           AND wro.organization_id = cic.organization_id(+)
           AND cic.cost_type_id(+) = 1
           --AND wro.wip_supply_type NOT IN (4, 6)
           AND we.ORGANIZATION_ID = ood.ORGANIZATION_ID
);

commit;
end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'fact_wo_requirments'
	and batch_name = 'wip';

commit;