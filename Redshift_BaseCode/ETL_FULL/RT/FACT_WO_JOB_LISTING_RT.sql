/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for RT reports.
# File Version: KPI v1.0
*/
begin;

drop table if exists bec_dwh_rpt.FACT_WO_JOB_LISTING_RT;

create table bec_dwh_rpt.FACT_WO_JOB_LISTING_RT diststyle auto sortkey(creation_date) as 
select distinct
    wjl.JOB_NAME,
    wjl.creation_date,
	wjl.created_by, 
	wjl.description,
	wjl.ASS_ITEM_ID,
	wjl.ASSEMBLY part_number,
	wjl.ASSEMBLY_DESCRIPTION part_desp,
	decode(firm,1,'Yes',2,'No') firm,
	mp.employee_id,  
	wjl.planner_code,
	pov.agent_name                      buyer, 
	JOB_TYPE,
	class_code,
	SCHEDULED_START_DATE,
	SCHEDULE_GROUP_NAME,
	date_released,
	scheduled_completion_date,
	ACT_DATE_COMPLETED date_completed,
	date_closed,
	SCHED_QTY start_quantity,
	decode(wjl.SCHED_QTY - wjl.QTY_COMPLETED - wjl.QTY_SCRAPPED, 0, NULL, wjl.SCHED_QTY - wjl.QTY_COMPLETED - wjl.QTY_SCRAPPED
    ) QUANTITY_REMAINING,
	QTY_COMPLETED,
	QTY_SCRAPPED,
	net_quantity,
	bom_revision,
	wjl.project_no,
    wjl.task_no, 
	JOB_STATUS,
	lu.meaning supply_type, 
	wjl.organization_id, 
	cic.item_cost, 
    wjl.SCHED_QTY * cic.item_cost ext_cost
	from 
	bec_dwh.FACT_WO_JOB_LISTING wjl,
	bec_ods.mtl_planners mp,
	bec_ods.po_agents_v   pov,
	bec_ods.cst_item_costs               cic,
	bec_ods.FND_LOOKUP_VALUES lu
	where 
	wjl.buyer_id = pov.agent_id (+)
	AND wjl.ASS_ITEM_ID = cic.inventory_item_id (+)
    AND wjl.organization_id = cic.organization_id (+)
    AND cic.cost_type_id (+) = 1 
    AND wjl.planner_code = mp.planner_code (+)
    AND wjl.organization_id = mp.organization_id (+)
    AND lu.lookup_type = 'WIP_SUPPLY'
    AND lu.lookup_code = wjl.wip_supply_type
;
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_wo_job_listing_rt' 
  and batch_name = 'wip';
commit;