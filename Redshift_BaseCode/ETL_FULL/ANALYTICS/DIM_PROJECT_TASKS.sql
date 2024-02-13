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

drop table if exists bec_dwh.dim_project_tasks;

create table bec_dwh.dim_project_tasks 
	distkey(project_id)
	sortkey (project_id,task_id)
as
(
select
	pt.project_id,
	PPA.SEGMENT1 Project_Number,
    PPA.NAME Project_Name,
	pp.person_id,
	pt.task_id,
	pt.parent_task_id,
	pt.task_number,
	pt.creation_date,
	pt.created_by,
	pt.last_update_date,
	pt.task_name,
	pt.wbs_level,
	pt.description "TASK_DESCRIPTION",
	pt.carrying_out_organization_id,
	pt.service_type_code,
	pt.chargeable_flag,
	pt.billable_flag,
	pt.start_date,
	pt.completion_date,
	pt.work_type_id,
	pt.long_task_name,
	pp.person_name "TASK_MANAGER",
	'N' as is_deleted_flg,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS') as source_app_id,
	(
	select
		system_id
	from
		bec_etl_ctrl.etlsourceappid
	where
		source_system = 'EBS')|| '-' || nvl(pt.project_id, 0)|| '-' || nvl(pt.task_id, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.PA_PROJECTS_ALL PPA
	inner join 
	bec_ods.pa_tasks pt on PPA.PROJECT_ID = PT.PROJECT_ID
left join 
	(
	select 
		person_id, 
		max (full_name) person_name
	from
		bec_ods.per_all_people_f
	group by
		person_id
	) pp
on
	pt.task_manager_person_id = pp.person_id
); 

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_project_tasks'
	and batch_name = 'ap';

commit;