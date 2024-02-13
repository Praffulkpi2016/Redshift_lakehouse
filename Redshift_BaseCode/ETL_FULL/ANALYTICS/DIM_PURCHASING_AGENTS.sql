/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

drop table if exists bec_dwh.DIM_PURCHASING_AGENTS;

create table bec_dwh.DIM_PURCHASING_AGENTS diststyle all sortkey(agent_id,location_id )
as 
(
select
	 a.agent_id,
	a.creation_date,
	a.last_update_date,
	b.location_code,
	b.location_id,
	a.authorization_limit,
	a.start_date_active,
	a.end_date_active,
	c.person_name agent_name,
	b.CREATED_BY,	 
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
    || '-'|| nvl(a.agent_id, 0)
	   as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
 
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
);

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_purchasing_agents'
	and batch_name = 'po';

commit;