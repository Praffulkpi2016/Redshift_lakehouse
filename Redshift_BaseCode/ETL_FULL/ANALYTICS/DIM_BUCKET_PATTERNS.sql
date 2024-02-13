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

drop table if exists bec_dwh.DIM_BUCKET_PATTERNS;

create table bec_dwh.DIM_BUCKET_PATTERNS diststyle all sortkey (BUCKET_PATTERN_ID)
as
(
select
    BUCKET_PATTERN_ID,
    BUCKET_PATTERN_NAME,
    NUMBER_DAILY_BUCKETS,
    NUMBER_WEEKLY_BUCKETS,
    NUMBER_MONTHLY_BUCKETS,
    NUMBER_QUARTERLY_BUCKETS,
    LAST_UPDATE_DATE,
    LAST_UPDATED_BY,
    CREATION_DATE,
    CREATED_BY,
    WEEK_START_DAY,
    DESCRIPTION,
    INACTIVE_DATE ,

-- Audit columns
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
    || '-'
       ||  nvl(BUCKET_PATTERN_ID, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.CHV_BUCKET_PATTERNS
);

end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_bucket_patterns'
	and batch_name = 'po';

commit;
