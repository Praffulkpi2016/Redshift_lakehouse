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

drop table if exists bec_dwh.dim_projects;

create table bec_dwh.dim_projects 
	diststyle all
	sortkey (project_id)
as
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
		source_system = 'EBS')|| '-' || nvl(project_id, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.pa_projects_all pap
);

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_projects'
	and batch_name = 'ap';

commit;