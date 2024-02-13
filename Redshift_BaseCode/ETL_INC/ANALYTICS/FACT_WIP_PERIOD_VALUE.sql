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
TRUNCATE bec_dwh.fact_wip_period_value_tmp;
--Insert records into temp table
INSERT INTO bec_dwh.fact_wip_period_value_tmp 
(
with totals as (
    select 
      wdj.organization_id, 
      wdj.WIP_ENTITY_ID, 
      oap2.ACCT_PERIOD_ID, 
      oap2.period_name, 
      sum(
        round(
          (
            nvl(wpb.tl_resource_in, 0)+ nvl(wpb.tl_overhead_in, 0)+ nvl(wpb.tl_outside_processing_in, 0)+ nvl(wpb.pl_material_in, 0)+ nvl(wpb.pl_resource_in, 0)+ nvl(wpb.pl_overhead_in, 0)+ nvl(wpb.pl_outside_processing_in, 0)+ nvl(wpb.pl_material_overhead_in, 0)+ nvl(wpb.tl_scrap_in, 0)
          ), 
          2
        )
      ) total_incured, 
      sum(
        round(
          (
            nvl(wpb.tl_material_var, 0)+ nvl(wpb.tl_resource_var, 0)+ nvl(wpb.tl_overhead_var, 0)+ nvl(
              wpb.tl_outside_processing_var, 0
            )+ nvl(wpb.pl_material_var, 0)+ nvl(wpb.pl_resource_var, 0)+ nvl(wpb.pl_overhead_var, 0)+ nvl(
              wpb.pl_outside_processing_var, 0
            )+ nvl(wpb.tl_material_overhead_var, 0) + nvl(wpb.pl_material_overhead_var, 0)+ nvl(wpb.tl_scrap_var, 0)
          ), 
          2
        )
      ) total_variance, 
      sum(
        round(
          (
            nvl(wpb.tl_resource_out, 0)+ nvl(wpb.tl_overhead_out, 0)+ nvl(
              wpb.tl_outside_processing_out, 0
            )+ nvl(wpb.pl_material_out, 0)+ nvl(wpb.pl_material_overhead_out, 0)+ nvl(wpb.pl_resource_out, 0)+ nvl(wpb.pl_overhead_out, 0)+ nvl(
              wpb.pl_outside_processing_out, 0
            )+ nvl(wpb.tl_material_overhead_out, 0)+ nvl(wpb.tl_material_out, 0)+ nvl(wpb.tl_scrap_out, 0)
          ), 
          2
        )
      ) total_relived 
    from 
      bec_ods.FND_LOOKUP_VALUES ml, 
      bec_ods.wip_period_balances wpb, 
      bec_ods.org_acct_periods oap, 
      bec_ods.mtl_system_items_b msi, 
      bec_ods.wip_entities we, 
      bec_ods.wip_discrete_jobs wdj, 
      bec_ods.ORG_ACCT_PERIODS oap2 
    where 
      wpb.wip_entity_id = wdj.wip_entity_id 
      and wpb.organization_id = wdj.organization_id 
      and ml.lookup_type = 'WIP_CLASS_TYPE' 
      and ml.lookup_code = wpb.class_type 
      and oap.organization_id = wdj.organization_id 
      and oap.acct_period_id = wpb.acct_period_id 
      and oap.schedule_close_date <= nvl(
        oap2.PERIOD_CLOSE_DATE, oap2.schedule_close_date
      ) 
      and we.wip_entity_id = wdj.wip_entity_id 
      and we.organization_id = wdj.organization_id 
      and wdj.primary_item_id = msi.inventory_item_id(+) 
      and msi.organization_id(+) = wdj.organization_id --and wdj.WIP_ENTITY_ID = 12693719
      --AND wdj.ORGANIZATION_ID  = 106 
      AND oap2.organization_id = wdj.ORGANIZATION_ID 
      AND oap2.period_start_date > sysdate - 365 
    group by 
      wdj.organization_id, 
      wdj.WIP_ENTITY_ID, 
      oap2.period_name, 
      oap2.ACCT_PERIOD_ID
  ), 
  wip_operations as (
    SELECT 
      MAX(operation_seq_num) operation_seq_num, 
      wip_entity_id 
    FROM 
      bec_ods.wip_operations 
    WHERE 
      date_last_moved IS NOT NULL 
    GROUP BY 
      wip_entity_id
  )
select distinct we.WIP_ENTITY_ID
FROM 
    bec_ods.wip_discrete_jobs wdj, 
    bec_ods.wip_entities we, 
    bec_ods.mtl_system_items_b msi, 
    bec_ods.wip_period_balances wpb, 
    bec_ods.org_organization_definitions ood, 
    bec_ods.fnd_lookup_values ml, 
    bec_ods.wip_schedule_groups wsg, 
    bec_ods.fnd_user fu, 
    totals, 
    wip_operations wo 
  WHERE 
    wdj.wip_entity_id = we.wip_entity_id 
    AND wdj.organization_id = we.organization_id 
    AND wdj.primary_item_id = msi.inventory_item_id (+) 
    AND wdj.organization_id = msi.organization_id (+) 
    AND wdj.organization_id = ood.organization_id 
    AND wdj.wip_entity_id = wpb.wip_entity_id 
    AND wdj.organization_id = wpb.organization_id 
    AND ml.lookup_type = 'WIP_CLASS_TYPE' 
    AND wpb.class_type = ml.lookup_code 
    AND wdj.schedule_group_id = wsg.schedule_group_id (+) 
    AND wdj.organization_id = wsg.organization_id (+) 
    AND wdj.created_by = fu.user_id 
    and totals.organization_id = wdj.organization_id 
    and totals.wip_entity_id = we.wip_entity_id 
    and totals.acct_period_id = wpb.acct_period_id 
    AND wdj.wip_entity_id = wo.wip_entity_id (+)
and (wdj.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_wip_period_value' and batch_name = 'inv')
or we.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_wip_period_value' and batch_name = 'inv')
or msi.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_wip_period_value' and batch_name = 'inv')
or wpb.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_wip_period_value' and batch_name = 'inv')
or ood.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_wip_period_value' and batch_name = 'inv')
or wsg.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_wip_period_value' and batch_name = 'inv')
)
);
--delete records from fact table
delete from bec_dwh.fact_wip_period_value
WHERE exists (select 1 from bec_dwh.fact_wip_period_value_tmp tmp
              where nvl(tmp.WIP_ENTITY_ID,0) = nvl(fact_wip_period_value.WIP_ENTITY_ID,0)
             );
-- Insert records into fact table
insert into bec_dwh.fact_wip_period_value 
(
  with totals as (
    select 
      wdj.organization_id, 
      wdj.WIP_ENTITY_ID, 
      oap2.ACCT_PERIOD_ID, 
      oap2.period_name, 
      sum(
        round(
          (
            nvl(wpb.tl_resource_in, 0)+ nvl(wpb.tl_overhead_in, 0)+ nvl(wpb.tl_outside_processing_in, 0)+ nvl(wpb.pl_material_in, 0)+ nvl(wpb.pl_resource_in, 0)+ nvl(wpb.pl_overhead_in, 0)+ nvl(wpb.pl_outside_processing_in, 0)+ nvl(wpb.pl_material_overhead_in, 0)+ nvl(wpb.tl_scrap_in, 0)
          ), 
          2
        )
      ) total_incured, 
      sum(
        round(
          (
            nvl(wpb.tl_material_var, 0)+ nvl(wpb.tl_resource_var, 0)+ nvl(wpb.tl_overhead_var, 0)+ nvl(
              wpb.tl_outside_processing_var, 0
            )+ nvl(wpb.pl_material_var, 0)+ nvl(wpb.pl_resource_var, 0)+ nvl(wpb.pl_overhead_var, 0)+ nvl(
              wpb.pl_outside_processing_var, 0
            )+ nvl(wpb.tl_material_overhead_var, 0) + nvl(wpb.pl_material_overhead_var, 0)+ nvl(wpb.tl_scrap_var, 0)
          ), 
          2
        )
      ) total_variance, 
      sum(
        round(
          (
            nvl(wpb.tl_resource_out, 0)+ nvl(wpb.tl_overhead_out, 0)+ nvl(
              wpb.tl_outside_processing_out, 0
            )+ nvl(wpb.pl_material_out, 0)+ nvl(wpb.pl_material_overhead_out, 0)+ nvl(wpb.pl_resource_out, 0)+ nvl(wpb.pl_overhead_out, 0)+ nvl(
              wpb.pl_outside_processing_out, 0
            )+ nvl(wpb.tl_material_overhead_out, 0)+ nvl(wpb.tl_material_out, 0)+ nvl(wpb.tl_scrap_out, 0)
          ), 
          2
        )
      ) total_relived 
    from 
      bec_ods.FND_LOOKUP_VALUES ml, 
      bec_ods.wip_period_balances wpb, 
      bec_ods.org_acct_periods oap, 
      bec_ods.mtl_system_items_b msi, 
      bec_ods.wip_entities we, 
      bec_ods.wip_discrete_jobs wdj, 
      bec_ods.ORG_ACCT_PERIODS oap2 
    where 
      wpb.wip_entity_id = wdj.wip_entity_id 
      and wpb.organization_id = wdj.organization_id 
      and ml.lookup_type = 'WIP_CLASS_TYPE' 
      and ml.lookup_code = wpb.class_type 
      and oap.organization_id = wdj.organization_id 
      and oap.acct_period_id = wpb.acct_period_id 
      and oap.schedule_close_date <= nvl(
        oap2.PERIOD_CLOSE_DATE, oap2.schedule_close_date
      ) 
      and we.wip_entity_id = wdj.wip_entity_id 
      and we.organization_id = wdj.organization_id 
      and wdj.primary_item_id = msi.inventory_item_id(+) 
      and msi.organization_id(+) = wdj.organization_id --and wdj.WIP_ENTITY_ID = 12693719
      --AND wdj.ORGANIZATION_ID  = 106 
      AND oap2.organization_id = wdj.ORGANIZATION_ID 
      AND oap2.period_start_date > sysdate - 365 
    group by 
      wdj.organization_id, 
      wdj.WIP_ENTITY_ID, 
      oap2.period_name, 
      oap2.ACCT_PERIOD_ID
  ), 
  wip_operations as (
    SELECT 
      MAX(operation_seq_num) operation_seq_num, 
      wip_entity_id 
    FROM 
      bec_ods.wip_operations 
    WHERE 
      date_last_moved IS NOT NULL 
    GROUP BY 
      wip_entity_id
  ) 
  SELECT 
    wdj.organization_id, 
    ood.organization_code, 
    ood.organization_name, 
    --oap.period_name,
    totals.period_name, 
    we.wip_entity_name, 
    we.wip_entity_id, 
    msi.segment1 assembly, 
    msi.description assembly_description, 
    we.creation_date, 
    wdj.actual_start_date, 
    ml.meaning class_type, 
    wdj.class_code class_code, 
    wsg.schedule_group_name, 
    wdj.scheduled_start_date, 
    wdj.scheduled_completion_date, 
    wdj.date_released, 
    wdj.date_completed, 
    wdj.date_closed, 
    wo.operation_seq_num, 
    fu.user_name created_by, 
    (
      SELECT 
        meaning 
      FROM 
        bec_ods.fnd_lookup_values 
      WHERE 
        lookup_type = 'WIP_JOB_STATUS' 
        AND lookup_code = wdj.status_type
    ) job_status_type, 
    wdj.start_quantity, 
    wdj.quantity_completed, 
    wdj.quantity_scrapped, 
    wdj.net_quantity, 
    totals.total_incured, 
    totals.total_relived, 
    totals.total_variance, 
    totals.total_incured - totals.total_relived - totals.total_variance ending_balance, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || wdj.organization_id organization_id_KEY, 
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || we.wip_entity_id wip_entity_id_KEY, 
    'N' AS IS_DELETED_FLG, 
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
    ) || '-' || nvl(we.wip_entity_id, 0) || '-' || nvl(
      totals.period_name, '1900-01-01 12:00:00'
    ) as dw_load_id, 
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
  FROM 
    bec_ods.wip_discrete_jobs wdj, 
    bec_ods.wip_entities we, 
    bec_ods.mtl_system_items_b msi, 
    bec_ods.wip_period_balances wpb, 
    bec_ods.org_organization_definitions ood, 
    bec_ods.fnd_lookup_values ml, 
    bec_ods.wip_schedule_groups wsg, 
    bec_ods.fnd_user fu, 
    totals, 
    wip_operations wo,
	bec_dwh.fact_wip_period_value_tmp tmp
  WHERE 
    wdj.wip_entity_id = we.wip_entity_id 
    AND wdj.organization_id = we.organization_id 
    AND wdj.primary_item_id = msi.inventory_item_id (+) 
    AND wdj.organization_id = msi.organization_id (+) 
    AND wdj.organization_id = ood.organization_id 
    AND wdj.wip_entity_id = wpb.wip_entity_id 
    AND wdj.organization_id = wpb.organization_id 
    AND ml.lookup_type = 'WIP_CLASS_TYPE' 
    AND wpb.class_type = ml.lookup_code 
    AND wdj.schedule_group_id = wsg.schedule_group_id (+) 
    AND wdj.organization_id = wsg.organization_id (+) 
    AND wdj.created_by = fu.user_id 
    and totals.organization_id = wdj.organization_id 
    and totals.wip_entity_id = we.wip_entity_id 
    and totals.acct_period_id = wpb.acct_period_id 
    AND wdj.wip_entity_id = wo.wip_entity_id (+)
	AND nvl(tmp.WIP_ENTITY_ID,0) = nvl(we.WIP_ENTITY_ID,0)
);
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_wip_period_value' 
  and batch_name = 'inv';
commit;