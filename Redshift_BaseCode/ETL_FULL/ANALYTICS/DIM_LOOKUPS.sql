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

drop table if exists bec_dwh.DIM_LOOKUPS;

create table bec_dwh.DIM_LOOKUPS 
	diststyle all 
	sortkey (lookup_type,lookup_code,language,view_application_id)
as
(
select 
	fnd.lookup_code as lookup_code,
	fnd.description as description,
	fnd.lookup_type as lookup_type,
	fnd.language as language,
	fnd.meaning as meaning,
	fnd.enabled_flag as enabled_flag,
	fnd.view_application_id as view_application_id,
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
		source_system = 'EBS')
		|| '-' || nvl(fnd.lookup_type, 'NA')|| '-' || nvl(fnd.lookup_code, 'NA')|| '-' || nvl(fnd.language, 'NA')|| '-' || nvl(fnd.view_application_id, 0)
				 as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.fnd_lookup_values fnd
where
	1 = 1
	and language = 'US'
	and enabled_flag = 'Y'
	and is_deleted_flg <> 'Y'
);

end;



update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_lookups'
	and batch_name = 'ap';

commit;