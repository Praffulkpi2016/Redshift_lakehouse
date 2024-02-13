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
TRUNCATE bec_dwh.fact_bom_overhead_usage_tmp;
--Insert records into temp table
INSERT INTO bec_dwh.fact_bom_overhead_usage_tmp 
(
select distinct bor.assembly_item_id, bor.organization_id, br.resource_id
FROM
    bec_ods.bom_operational_routings bor,
    bec_ods.bom_operation_sequences bos,
    bec_ods.bom_standard_operations bso,
    bec_ods.bom_operation_resources br
WHERE
    bor.routing_sequence_id = bos.routing_sequence_id (+)
    AND bos.operation_sequence_id = br.operation_sequence_id (+)
    AND bos.standard_operation_id = bso.standard_operation_id (+)
    AND bos.disable_date IS NULL
and (bor.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_bom_overhead_usage' and batch_name = 'wip')
or bos.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_bom_overhead_usage' and batch_name = 'wip')
or bso.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_bom_overhead_usage' and batch_name = 'wip')
or br.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='fact_bom_overhead_usage' and batch_name = 'wip')
)
);
--delete records from fact table
delete from bec_dwh.fact_bom_overhead_usage
WHERE exists (select 1 from bec_dwh.fact_bom_overhead_usage_tmp tmp
              where nvl(tmp.assembly_item_id,0) = nvl(fact_bom_overhead_usage.assembly_item_id,0)
			  and nvl(tmp.resource_id,0) = nvl(fact_bom_overhead_usage.resource_id,0)
			  and nvl(tmp.organization_id,0) = nvl(fact_bom_overhead_usage.organization_id,0)
             );
-- Insert records into fact table
insert into bec_dwh.fact_bom_overhead_usage 
(
  SELECT
    bor.assembly_item_id,
    bor.organization_id,
    bor.alternate_routing_designator,
    bso.operation_code standard_operation_code,
    bos.operation_seq_num,
    bos.operation_description,
	bos.department_id,
    br.resource_seq_num,
	br.resource_id,
    ml1.meaning basis_type,
    br.usage_rate_or_amount * 60 usage_in_mins,
    br.usage_rate_or_amount usage_in_hrs,
    bos.effectivity_date,
    (
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || bor.assembly_item_id assembly_item_id_KEY,
		(
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || bor.organization_id organization_id_KEY, 
	(
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || bos.department_id department_id_KEY, 
	(
      select 
        system_id 
      from 
        bec_etl_ctrl.etlsourceappid 
      where 
        source_system = 'EBS'
    )|| '-' || br.resource_id resource_id_KEY, 	
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
    ) || '-' || nvl(bor.assembly_item_id, 0)
	|| '-' || nvl(bor.organization_id, 0)
	|| '-' || nvl(br.resource_id, 0)
	|| '-' || nvl(bos.operation_seq_num, 0)
	|| '-' || nvl(br.resource_seq_num, 0)
	|| '-' || nvl(bor.alternate_routing_designator, 'NA')
	|| '-' || nvl(bos.effectivity_date, '1900-01-01 12:00:00') as dw_load_id, 
    getdate() as dw_insert_date, 
    getdate() as dw_update_date 
  FROM
    (select * from bec_ods.bom_operational_routings where is_deleted_flg <> 'Y') bor,
    (select * from bec_ods.bom_operation_sequences where is_deleted_flg <> 'Y') bos,
    (select * from bec_ods.bom_standard_operations where is_deleted_flg <> 'Y') bso,
    (select * from bec_ods.bom_operation_resources where is_deleted_flg <> 'Y') br,
    (select * from bec_ods.fnd_lookup_values where is_deleted_flg <> 'Y') ml1,
	bec_dwh.fact_bom_overhead_usage_tmp tmp
WHERE
        bor.routing_sequence_id = bos.routing_sequence_id (+)
    AND bos.operation_sequence_id = br.operation_sequence_id (+)
    AND ml1.lookup_type (+) = 'BOM_BASIS_TYPE'
    AND br.basis_type = ml1.lookup_code (+)
    AND bos.standard_operation_id = bso.standard_operation_id (+)
    AND bos.disable_date IS NULL
	AND nvl(tmp.assembly_item_id, 0) = nvl(bor.assembly_item_id, 0)
	AND nvl(tmp.resource_id, 0) = nvl(br.resource_id, 0)
	AND nvl(tmp.organization_id, 0) = nvl(bor.organization_id, 0)
);
  
end;
update 
  bec_etl_ctrl.batch_dw_info 
set 
  load_type = 'I', 
  last_refresh_date = getdate() 
where 
  dw_table_name = 'fact_bom_overhead_usage' 
  and batch_name = 'wip';
commit;