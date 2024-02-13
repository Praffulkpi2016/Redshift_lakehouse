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

drop table if exists bec_dwh.DIM_AR_AGING_BUCKETS;

create table bec_dwh.DIM_AR_AGING_BUCKETS diststyle all sortkey(AGING_BUCKET_ID,
BUCKET_NAME,
BUCKET_SEQUENCE_NUM)
as
(
select
	a.AGING_BUCKET_ID,
	a.BUCKET_NAME,
	b.BUCKET_SEQUENCE_NUM,
	b.DAYS_START ,
	b.DAYS_TO ,
	b."TYPE",
	b.last_update_date,
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
    || '-' || nvl(a.AGING_BUCKET_ID, 0)
	|| '-' || nvl(a.BUCKET_NAME, 'NA')
	|| '-' || nvl(b.BUCKET_SEQUENCE_NUM, 0) as dw_load_id,
	getdate() as dw_insert_date,
	getdate() as dw_update_date
from
	bec_ods.AR_AGING_BUCKETS a,
	bec_ods.AR_AGING_BUCKET_LINES_B b
where
	a.AGING_BUCKET_ID = b.AGING_BUCKET_ID
order by
	a.AGING_BUCKET_ID,
	a.BUCKET_NAME,
	b.BUCKET_SEQUENCE_NUM
);
end;


update
	bec_etl_ctrl.batch_dw_info
set
	load_type = 'I',
	last_refresh_date = getdate()
where
	dw_table_name = 'dim_ar_aging_buckets'
	and batch_name = 'ar';

commit;