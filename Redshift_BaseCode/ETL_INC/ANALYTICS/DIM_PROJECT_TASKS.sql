/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for Dimensions.
# File Version: KPI v1.0
*/

begin;

-- Delete Records
DELETE FROM bec_dwh.DIM_PROJECT_TASKS
WHERE EXISTS (
    SELECT 1
    FROM bec_ods.PA_PROJECTS_ALL PPA
	INNER JOIN bec_ods.pa_tasks pt on PPA.PROJECT_ID = PT.PROJECT_ID
    LEFT JOIN (
        SELECT person_id, MAX(full_name) person_name
        FROM bec_ods.per_all_people_f
        GROUP BY person_id
    ) pp ON pt.task_manager_person_id = pp.person_id
    WHERE NVL(dim_project_tasks.project_id, 0) = NVL(PPA.project_id, 0)
    AND NVL(dim_project_tasks.task_id, 0) = NVL(pt.task_id, 0)
    AND pt.kca_seq_date > (
        SELECT (executebegints - prune_days)
        FROM bec_etl_ctrl.batch_dw_info
        WHERE dw_table_name = 'dim_project_tasks'
        AND batch_name = 'ap'
    )
);

commit;

-- Insert records

insert into bec_dwh.dim_project_tasks 
(
project_id
,Project_Number
,Project_Name
,person_id
,task_id
,parent_task_id
,task_number
,creation_date
,created_by
,last_update_date
,task_name
,wbs_level
,task_description
,carrying_out_organization_id
,service_type_code
,chargeable_flag
,billable_flag
,start_date
,completion_date
,work_type_id
,long_task_name
,task_manager
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
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
	-- audit column
	'N' as is_deleted_flg,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') as source_app_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(pt.project_id,0)||'-'||nvl(pt.task_id,0) as dw_load_id,
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
	from bec_ods.per_all_people_f 
	group by person_id
	) pp
on pt.task_manager_person_id = pp.person_id
where 1=1
and (pt.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_project_tasks' and batch_name = 'ap')
 )
);
 commit;
-- Soft delete

update bec_dwh.dim_project_tasks set is_deleted_flg = 'Y'
WHERE NOT EXISTS (
    SELECT 1
    FROM bec_ods.PA_PROJECTS_ALL PPA
	INNER JOIN bec_ods.pa_tasks pt on PPA.PROJECT_ID = PT.PROJECT_ID
    LEFT JOIN (
        SELECT person_id, MAX(full_name) person_name
        FROM bec_ods.per_all_people_f
        GROUP BY person_id
    ) pp ON pt.task_manager_person_id = pp.person_id
    WHERE NVL(dim_project_tasks.project_id, 0) = NVL(PPA.project_id, 0)
    AND NVL(dim_project_tasks.task_id, 0) = NVL(pt.task_id, 0)
    AND (ppa.is_deleted_flg <> 'Y' or pt.is_deleted_flg <> 'Y')
);

commit;

end;

UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_project_tasks'
and batch_name = 'ap';

COMMIT;