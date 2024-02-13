/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach for Dimensions.
# File Version: KPI v1.0
*/
begin;

-- Delete Records

delete from bec_dwh.dim_purchasing_agents
where nvl(agent_id,0) in (
select nvl(ods.agent_id,0) as agent_id from bec_dwh.dim_purchasing_agents dw,
 (select 
	trunc(a.agent_id) as  agent_id
	FROM bec_ods.po_agents a,
          bec_ods.hr_locations_all b,
          (SELECT 
    PERSON_ID, max(full_name) PERSON_NAME
FROM      
bec_ods.PER_ALL_PEOPLE_F 
group by person_id ) c
    where 1=1
AND a.agent_id = c.person_id
and a.location_id = b.location_id(+)
and (  a.kca_seq_date   > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name  ='dim_purchasing_agents' and batch_name = 'po') or 
 b.kca_seq_date  > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name  ='dim_purchasing_agents' and batch_name = 'po')
 
)
	) ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') ||'-'||nvl(ods.agent_id,0)
);

commit;

-- Insert records

insert into bec_dwh.DIM_PURCHASING_AGENTS
(
     agent_id,
	 creation_date,
	 last_update_date,
	 location_code,
	 location_id,
	 authorization_limit,
	 start_date_active,
	 end_date_active,
	 agent_name ,
	 CREATED_BY,
	 is_deleted_flg,
	 source_app_id,
	 dw_load_id,
	 dw_insert_date,
	 dw_update_date
)
(
select 
	trunc(a.agent_id) as  agent_id,
	 a.creation_date  as creation_date,
	a.last_update_date as last_update_date,
	b.location_code as location_code,
	b.location_id,	 
	a.authorization_limit as  authorization_limit,
	a.start_date_active as start_date_active,
	a.end_date_active as end_date_active,
	c.person_name as  agent_name,
	b.CREATED_BY as CREATED_BY, 	
'N' as is_deleted_flg,
 (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )                   AS source_app_id,
    (
        SELECT
            system_id
        FROM
            bec_etl_ctrl.etlsourceappid
        WHERE
            source_system = 'EBS'
    )
    || '-'
       || nvl(a.agent_id,0)  AS dw_load_id,
    getdate()           AS dw_insert_date,
    getdate()           AS dw_update_date
  FROM bec_ods.po_agents a,
          bec_ods.hr_locations_all b,
          (SELECT 
    PERSON_ID, max(full_name) PERSON_NAME
FROM      
bec_ods.PER_ALL_PEOPLE_F 
group by person_id ) c
    where 1=1
--and b.location_id = a.location_id(+)
AND a.agent_id = c.person_id
and a.location_id = b.location_id(+)
and ( a.kca_seq_date   > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name  ='dim_purchasing_agents' and batch_name = 'po') or 
 b.kca_seq_date  > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name  ='dim_purchasing_agents' and batch_name = 'po')
)
);

-- Soft delete

update bec_dwh.dim_purchasing_agents set is_deleted_flg = 'Y'
where nvl(agent_id,0) not in (
select nvl(ods.agent_id,0) as agent_id from bec_dwh.dim_purchasing_agents dw, 
(select 
	trunc(a.agent_id) as agent_id,a.kca_operation
	FROM (select * from bec_ods.po_agents WHERE is_deleted_flg <> 'Y') a,
          (select * from bec_ods.hr_locations_all WHERE is_deleted_flg <> 'Y') b,
          (SELECT 
    PERSON_ID, max(full_name) PERSON_NAME
FROM      
bec_ods.PER_ALL_PEOPLE_F 
where is_deleted_flg <> 'Y'
group by person_id ) c
    where 1=1
--and b.location_id = a.location_id(+)
AND a.agent_id = c.person_id
and a.location_id = b.location_id(+)
	) ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.agent_id,0)  
);

commit;

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_purchasing_agents' and batch_name = 'po';

COMMIT;