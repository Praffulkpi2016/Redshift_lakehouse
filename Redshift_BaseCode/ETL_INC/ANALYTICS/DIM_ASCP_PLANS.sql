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

truncate table bec_dwh.DIM_ASCP_PLANS;

insert into bec_dwh.DIM_ASCP_PLANS
(
select
	plan_id,
    compile_designator,
    sr_instance_id,
    description,
    curr_start_date,
    curr_cutoff_date,
    cutoff_date,
    curr_plan_type,
    data_start_date,
    data_completion_date,
    daily_cutoff_bucket,
    weekly_cutoff_bucket,
	case when compile_designator in ('BE-GLOBAL','MPS-Plan-1','MRP-Plan-1') 
	then 'Y' else 'N' end as LOAD_FLG,
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
		source_system = 'EBS')|| '-' || nvl(plan_id, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.msc_plans
where is_deleted_flg <> 'Y'
);

commit;

end;

update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_ascp_plans'
	and batch_name = 'ascp';

commit;