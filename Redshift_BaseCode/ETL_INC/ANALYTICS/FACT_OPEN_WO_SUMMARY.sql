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
TRUNCATE bec_dwh.fact_open_wo_summary_tmp;
--Insert records into temp table
INSERT INTO bec_dwh.fact_open_wo_summary_tmp 
(
select distinct b.WIP_ENTITY_ID
FROM
    (select * from bec_ods.wip_operations where is_deleted_flg <> 'Y') a,
    (select * from bec_ods.wip_entities where is_deleted_flg <> 'Y') b,
    (select wip_entity_id,primary_item_id,organization_id,net_quantity,quantity_completed,kca_seq_date from bec_ods.wip_discrete_jobs where is_deleted_flg <> 'Y' 
    and  status_type = 3)    c,
    (select * from bec_dwh.dim_inv_item_category_set where item_category_segment1 = 'Stack') d
WHERE a.wip_entity_id = b.wip_entity_id
    AND b.wip_entity_id = c.wip_entity_id
    AND c.primary_item_id = d.inventory_item_id
    AND c.organization_id = d.organization_id
and (a.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_open_wo_summary' and batch_name = 'wip')
or b.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_open_wo_summary' and batch_name = 'wip')
or c.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_open_wo_summary' and batch_name = 'wip')
)
);
--delete records from fact table
delete from bec_dwh.fact_open_wo_summary
WHERE exists (select 1 from bec_dwh.fact_open_wo_summary_tmp tmp
              where nvl(tmp.WIP_ENTITY_ID,0) = nvl(fact_open_wo_summary.WIP_ENTITY_ID,0)
             );
-- Insert records into fact table
insert into bec_dwh.fact_open_wo_summary 
(
  select 
wip_entity_id	
,primary_item_id	
,organization_id	
,work_order	
,assembly_quantity	
,assembly_completed_qty	
,operation_seq_num	
,quantity_in_queue	
,quantity_running	
,quantity_scrapped	
,quantity_waiting_to_move	
,quantity_rejected	
,quantity_completed
--audit COLUMNS
,'N' as is_deleted_flg,
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
    || '-' || nvl(wip_entity_id,0)
	|| '-' || nvl(operation_seq_num,0)
	as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from 
(
SELECT
    b.WIP_ENTITY_ID,    
	c.primary_item_id ,
	c.organization_id,
	b.wip_entity_name               work_order,
    --msi.segment1                    assembly_pn,
    c.net_quantity                  assembly_quantity,
    c.quantity_completed            assembly_completed_qty,
    a.operation_seq_num,
    SUM(a.quantity_in_queue)        quantity_in_queue,
    SUM(a.quantity_running)         quantity_running,
    SUM(a.quantity_scrapped)        quantity_scrapped,
    SUM(a.quantity_waiting_to_move) quantity_waiting_to_move,
    SUM(a.quantity_rejected)        quantity_rejected,
    SUM(a.quantity_completed)       quantity_completed
FROM
    (select * from bec_ods.wip_operations where is_deleted_flg <> 'Y') a,
    (select * from bec_ods.wip_entities where is_deleted_flg <> 'Y') b,
    (select wip_entity_id,primary_item_id,organization_id,net_quantity,quantity_completed from bec_ods.wip_discrete_jobs where is_deleted_flg <> 'Y' 
    and  status_type = 3   )    c,
   -- bec_ods.MTL_SYSTEM_ITEMS_B    msi,
    (select * from bec_dwh.dim_inv_item_category_set where item_category_segment1 = 'Stack') d,
	bec_dwh.fact_open_wo_summary_tmp tmp
WHERE
        a.wip_entity_id = b.wip_entity_id
    AND b.wip_entity_id = c.wip_entity_id
    --AND msi.inventory_item_id = c.primary_item_id
    --AND msi.organization_id = c.organization_id
--    AND c.status_type = 3                                --Released status
    AND c.primary_item_id = d.inventory_item_id
    AND c.organization_id = d.organization_id
--    AND d.item_category_segment1 = 'Stack'
	AND nvl(tmp.WIP_ENTITY_ID,0) = nvl(b.WIP_ENTITY_ID,0)
GROUP BY
    a.operation_seq_num,
    --msi.segment1,
    b.WIP_ENTITY_ID,
	c.primary_item_id ,
	c.organization_id,
    b.wip_entity_name,
    c.net_quantity,
    c.quantity_completed
 )
);
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_open_wo_summary' 
  and batch_name = 'wip';
commit;