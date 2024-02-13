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

delete from bec_dwh.dim_projects
where (nvl(project_id,0))  in (
select nvl(ods.project_id,0) as project_id 
from bec_dwh.dim_projects dw, bec_ods.pa_projects_all ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.project_id,0)
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_projects' and batch_name = 'ap')
 )
);

commit;

-- Insert records

insert into bec_dwh.dim_projects
(
project_id,
name,
project_num,
project_type,
project_status_code,
description,
start_date,
completion_date,
enabled_flag,
attribute_category,
org_id,
project_currency_code,
long_name,
last_update_date,
is_deleted_flg,
source_app_id,
dw_load_id,
dw_insert_date,
dw_update_date
)
(
select
	project_id as project_id,
	name as name,
	segment1 as project_num,
	project_type as project_type,
	project_status_code as project_status_code,
	description as description,
	start_date as start_date,
	completion_date as completion_date,
	enabled_flag as enabled_flag,
	attribute_category as attribute_category,
	org_id as org_id,
	project_currency_code as project_currency_code,
	long_name as long_name,
	last_update_date as last_update_date,
	-- audit columns
	'N' as is_deleted_flg,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS') as source_app_id,
	(select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(project_id,0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from bec_ods.pa_projects_all 
WHERE 1=1
and (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info where dw_table_name ='dim_projects' and batch_name = 'ap')
 )
);
 
-- Soft delete

update bec_dwh.dim_projects set is_deleted_flg = 'Y'
where (nvl(project_id,0)) not in (
select nvl(ods.project_id,0) as project_id 
from bec_dwh.dim_projects dw, bec_ods.pa_projects_all ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.project_id,0)
AND ods.is_deleted_flg<> 'Y'
);
commit;

end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_projects'
and batch_name = 'ap';

COMMIT;