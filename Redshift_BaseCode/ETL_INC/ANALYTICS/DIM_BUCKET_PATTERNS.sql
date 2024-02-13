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

delete from bec_dwh.dim_bucket_patterns
where BUCKET_PATTERN_ID in (
select ods.BUCKET_PATTERN_ID from bec_dwh.dim_bucket_patterns dw, bec_ods.CHV_BUCKET_PATTERNS ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.BUCKET_PATTERN_ID,0) 
and (ods.kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='dim_bucket_patterns' and batch_name = 'po')
 )
);

commit;





-- Insert records

insert into bec_dwh.dim_bucket_patterns
(
    BUCKET_PATTERN_ID
    ,BUCKET_PATTERN_NAME
    ,NUMBER_DAILY_BUCKETS
    ,NUMBER_WEEKLY_BUCKETS
    ,NUMBER_MONTHLY_BUCKETS
    ,NUMBER_QUARTERLY_BUCKETS
    ,LAST_UPDATE_DATE
    ,LAST_UPDATED_BY
    ,CREATION_DATE
    ,CREATED_BY
    ,WEEK_START_DAY
    ,DESCRIPTION
    ,INACTIVE_DATE 
,is_deleted_flg
,source_app_id
,dw_load_id
,dw_insert_date
,dw_update_date
)
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
	where (kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_dw_info 
where dw_table_name ='dim_bucket_patterns' and batch_name = 'ap')
 )
 );






-- Soft delete

update bec_dwh.dim_bucket_patterns set is_deleted_flg = 'Y'
where BUCKET_PATTERN_ID not in (
select ods.BUCKET_PATTERN_ID from bec_dwh.dim_bucket_patterns dw,  bec_ods.CHV_BUCKET_PATTERNS ods
where dw.dw_load_id = (select system_id from bec_etl_ctrl.etlsourceappid where source_system='EBS')||'-'||nvl(ods.BUCKET_PATTERN_ID,0) 
AND ods.is_deleted_flg <> 'Y');

commit;

END;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_bucket_patterns' and batch_name = 'po';

COMMIT;