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
--delete records from temp table
TRUNCATE bec_dwh.fact_wo_job_listing_tmp;
--Insert records into temp table
INSERT INTO bec_dwh.fact_wo_job_listing_tmp 
(
select distinct DJ.WIP_ENTITY_ID
FROM   
       (SELECT * FROM bec_ods.WIP_DISCRETE_JOBS WHERE IS_DELETED_FLG <> 'Y') DJ,
	   (SELECT WIP_ENTITY_ID,ORGANIZATION_ID,WIP_ENTITY_NAME,ENTITY_TYPE,CREATED_BY,kca_seq_date
	   FROM bec_ods.WIP_ENTITIES WHERE IS_DELETED_FLG <> 'Y') WE,
       (SELECT INVENTORY_ITEM_ID,ORGANIZATION_ID,SEGMENT1,DESCRIPTION,
PRIMARY_UOM_CODE,planner_code,buyer_id,kca_seq_date FROM bec_ods.MTL_SYSTEM_ITEMS_B WHERE IS_DELETED_FLG <> 'Y') MSI,
       (SELECT * FROM bec_ods.WIP_SCHEDULE_GROUPS WHERE IS_DELETED_FLG <> 'Y') SG,
       (SELECT REQUIRED_QUANTITY,QUANTITY_ISSUED,OPERATION_SEQ_NUM,WIP_ENTITY_ID,
	   INVENTORY_ITEM_ID,ORGANIZATION_ID,REPETITIVE_SCHEDULE_ID,kca_seq_date
	   FROM bec_ods.WIP_REQUIREMENT_OPERATIONS WHERE IS_DELETED_FLG <> 'Y') WRO
WHERE  1=1
AND    DJ.WIP_ENTITY_ID   = WE.WIP_ENTITY_ID 
AND    DJ.ORGANIZATION_ID = WE.ORGANIZATION_ID 
AND    DJ.ORGANIZATION_ID = SG.ORGANIZATION_ID(+)
AND    DJ.SCHEDULE_GROUP_ID  = SG.SCHEDULE_GROUP_ID(+)
AND    DJ.PRIMARY_ITEM_ID = MSI.INVENTORY_ITEM_ID(+)
AND    DJ.ORGANIZATION_ID = MSI.ORGANIZATION_ID(+)
AND    DJ.WIP_ENTITY_ID = WRO.WIP_ENTITY_ID(+)
AND    DJ.ORGANIZATION_ID = WRO.ORGANIZATION_ID (+)
and (DJ.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_wo_job_listing' and batch_name = 'wip')
or WE.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_wo_job_listing' and batch_name = 'wip')
or MSI.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_wo_job_listing' and batch_name = 'wip')
or SG.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_wo_job_listing' and batch_name = 'wip')
or WRO.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_wo_job_listing' and batch_name = 'wip')
)
);
--delete records from fact table
delete from bec_dwh.fact_wo_job_listing
WHERE exists (select 1 from bec_dwh.fact_wo_job_listing_tmp tmp
              where nvl(tmp.WIP_ENTITY_ID,0) = nvl(fact_wo_job_listing.WIP_ENTITY_ID,0)
             );
-- Insert records into fact table
insert into bec_dwh.fact_wo_job_listing 
(
SELECT DJ.WIP_ENTITY_ID,
       DJ.ORGANIZATION_ID ,
	   	 (
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || DJ.ORGANIZATION_ID as organization_id_key,
       DJ.PRIMARY_ITEM_ID ASS_ITEM_ID,
	   MSI1.INVENTORY_ITEM_ID COMP_ITEM_ID,
	   SG.SCHEDULE_GROUP_ID,
       WE.WIP_ENTITY_NAME JOB_NAME,
       ML1.MEANING JOB_TYPE,
       MSI.SEGMENT1 ASSEMBLY,
       MSI.DESCRIPTION ASSEMBLY_DESCRIPTION,
       MSI.PRIMARY_UOM_CODE UOM,
       ML2.MEANING JOB_STATUS,
	   MSI1.SEGMENT1 COMPONENT_ITEM,
	   MSI1.DESCRIPTION COMPONENT_ITEM_DESCRIPTION,
	   WE.ENTITY_TYPE,
	   WE.CREATED_BY,
	   WRO.REQUIRED_QUANTITY,
	   WRO.QUANTITY_ISSUED,
	   WRO.OPERATION_SEQ_NUM,
       DJ.START_QUANTITY SCHED_QTY,
       DJ.QUANTITY_COMPLETED QTY_COMPLETED,
       DJ.QUANTITY_SCRAPPED QTY_SCRAPPED,
       DJ.SCHEDULED_START_DATE SCHEDULED_START_DATE,
       DJ.SCHEDULED_COMPLETION_DATE SCHEDULED_COMPLETION_DATE,
       SG.SCHEDULE_GROUP_NAME SCHEDULE_GROUP_NAME,
       DJ.BUILD_SEQUENCE BUILD_SEQUENCE, 
	   DJ.DATE_COMPLETED ACT_DATE_COMPLETED,
       DJ.DATE_CLOSED DATE_CLOSED,
	   	   DJ.CREATION_DATE,
	   DJ.DATE_RELEASED,
	  --added for quick site
	   dj.description,
	   dj.firm_planned_flag firm,
	   msi.planner_code,
	   dj.class_code,
	   dj.net_quantity,
	   dj.bom_revision,
	   dj.wip_supply_type,
	   dj.project_id,
	   dj.task_id,
	   dj.attribute1 project_no,
	   dj.attribute2 task_no,
	   msi.buyer_id,
	   WRO.REPETITIVE_SCHEDULE_ID,
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
    || '-' || nvl(MSI1.INVENTORY_ITEM_ID, 0)
	|| '-' || nvl(DJ.ORGANIZATION_ID, 0)
	|| '-' || nvl(DJ.WIP_ENTITY_ID, 0)
	||'-'||nvl(WRO.OPERATION_SEQ_NUM,0)
		||'-'||nvl(SG.SCHEDULE_GROUP_ID,0)
		||'-'||nvl(WRO.REPETITIVE_SCHEDULE_ID,0)
	   as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
FROM   
       (SELECT * FROM bec_ods.WIP_DISCRETE_JOBS WHERE IS_DELETED_FLG <> 'Y') DJ,
	   (SELECT WIP_ENTITY_ID,ORGANIZATION_ID,WIP_ENTITY_NAME,ENTITY_TYPE,CREATED_BY
	   FROM bec_ods.WIP_ENTITIES WHERE IS_DELETED_FLG <> 'Y') WE,
       (SELECT INVENTORY_ITEM_ID,ORGANIZATION_ID,SEGMENT1,DESCRIPTION,
PRIMARY_UOM_CODE,planner_code,buyer_id	   FROM bec_ods.MTL_SYSTEM_ITEMS_B WHERE IS_DELETED_FLG <> 'Y') MSI,
	   (SELECT INVENTORY_ITEM_ID,ORGANIZATION_ID,SEGMENT1,DESCRIPTION FROM bec_ods.MTL_SYSTEM_ITEMS_B WHERE IS_DELETED_FLG <> 'Y') MSI1,
       (SELECT * FROM bec_ods.WIP_SCHEDULE_GROUPS WHERE IS_DELETED_FLG <> 'Y') SG,
       (SELECT REQUIRED_QUANTITY,QUANTITY_ISSUED,OPERATION_SEQ_NUM,WIP_ENTITY_ID,
	   INVENTORY_ITEM_ID,ORGANIZATION_ID,REPETITIVE_SCHEDULE_ID
	   FROM bec_ods.WIP_REQUIREMENT_OPERATIONS WHERE IS_DELETED_FLG <> 'Y') WRO,
	   (SELECT MEANING,LOOKUP_CODE FROM bec_ods.FND_LOOKUP_VALUES WHERE IS_DELETED_FLG <> 'Y'
	   AND LOOKUP_TYPE = 'WIP_DISCRETE_JOB') ML1,
       (SELECT MEANING,LOOKUP_CODE FROM bec_ods.FND_LOOKUP_VALUES WHERE IS_DELETED_FLG <> 'Y'
	   AND LOOKUP_TYPE = 'WIP_JOB_STATUS') ML2,
	   bec_dwh.fact_wo_job_listing_tmp tmp
WHERE  1=1
AND    DJ.WIP_ENTITY_ID   = WE.WIP_ENTITY_ID 
AND    DJ.ORGANIZATION_ID = WE.ORGANIZATION_ID 
AND    DJ.ORGANIZATION_ID = SG.ORGANIZATION_ID(+)
AND    DJ.SCHEDULE_GROUP_ID  = SG.SCHEDULE_GROUP_ID(+)
AND    DJ.PRIMARY_ITEM_ID = MSI.INVENTORY_ITEM_ID(+)
AND    DJ.ORGANIZATION_ID = MSI.ORGANIZATION_ID(+)
AND    DJ.WIP_ENTITY_ID = WRO.WIP_ENTITY_ID(+)
AND    DJ.ORGANIZATION_ID = WRO.ORGANIZATION_ID (+)
AND    WRO.INVENTORY_ITEM_ID = MSI1.INVENTORY_ITEM_ID(+)
AND    WRO.ORGANIZATION_ID = MSI1.ORGANIZATION_ID(+)
AND    DJ.JOB_TYPE= ML1.LOOKUP_CODE 
AND    DJ.STATUS_TYPE = ML2.LOOKUP_CODE 
AND	   nvl(tmp.WIP_ENTITY_ID,0) = nvl(DJ.WIP_ENTITY_ID,0)
);
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_wo_job_listing' 
  and batch_name = 'wip';
commit;