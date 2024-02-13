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

delete
from
	bec_dwh.DIM_AR_AGING_BUCKETS
where
	(nvl(AGING_BUCKET_ID,0),
	nvl(BUCKET_NAME,'NA'),
	nvl(BUCKET_SEQUENCE_NUM,0)) in
(
	select
		nvl(ods.AGING_BUCKET_ID,0) as AGING_BUCKET_ID,
		nvl(ods.BUCKET_NAME,'NA') as BUCKET_NAME,
		nvl(ods.BUCKET_SEQUENCE_NUM,0) as BUCKET_SEQUENCE_NUM
	from
		bec_dwh.DIM_AR_AGING_BUCKETS dw,
		(
		select
			a.AGING_BUCKET_ID,
			a.BUCKET_NAME,
			b.BUCKET_SEQUENCE_NUM
		from
			bec_ods.AR_AGING_BUCKETS a,
			bec_ods.AR_AGING_BUCKET_LINES_B b
		where
			a.AGING_BUCKET_ID = b.AGING_BUCKET_ID
						and (a.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_ar_aging_buckets'
				and batch_name = 'ar')
				 
				or b.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_ar_aging_buckets'
				and batch_name = 'ar')
				 )
		) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')
|| '-' || nvl(ods.AGING_BUCKET_ID, 0)
|| '-' || nvl(ods.BUCKET_NAME, 'NA')
|| '-' || nvl(ods.BUCKET_SEQUENCE_NUM, 0)
);

commit;
-- Insert records

insert
	into
	bec_dwh.DIM_AR_AGING_BUCKETS
(
aging_bucket_id,
	bucket_name,
	bucket_sequence_num,
	days_start,
	days_to,
	"type",
	last_update_date,
	is_deleted_flg,
	source_app_id,
	dw_load_id,
	dw_insert_date,
	dw_update_date
)
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
						and (a.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_ar_aging_buckets'
				and batch_name = 'ar')
				 
				or b.kca_seq_date > (
			select
				(executebegints-prune_days)
			from
				bec_etl_ctrl.batch_dw_info
			where
				dw_table_name = 'dim_ar_aging_buckets'
				and batch_name = 'ar')
				 )
 );
-- Soft delete

update
	bec_dwh.DIM_AR_AGING_BUCKETS
set
	is_deleted_flg = 'Y'
where
	(nvl(AGING_BUCKET_ID,0),
	nvl(BUCKET_NAME,'NA'),
	nvl(BUCKET_SEQUENCE_NUM,0)) not in (
	select
		nvl(ods.AGING_BUCKET_ID,0) as AGING_BUCKET_ID,
		nvl(ods.BUCKET_NAME,'NA') as BUCKET_NAME,
		nvl(ods.BUCKET_SEQUENCE_NUM,0) as BUCKET_SEQUENCE_NUM
	from
		bec_dwh.DIM_AR_AGING_BUCKETS dw,
		(
		select
			a.AGING_BUCKET_ID,
			a.BUCKET_NAME,
			b.BUCKET_SEQUENCE_NUM,
			b.last_update_date,
			b.kca_operation
		from
			(select * from bec_ods.AR_AGING_BUCKETS where is_deleted_flg <> 'Y') a,
			(select * from bec_ods.AR_AGING_BUCKET_LINES_B where is_deleted_flg <> 'Y') b
		where
			a.AGING_BUCKET_ID = b.AGING_BUCKET_ID
		) ods
	where
		dw.dw_load_id = (
		select
			system_id
		from
			bec_etl_ctrl.etlsourceappid
		where
			source_system = 'EBS')
|| '-' || nvl(ods.AGING_BUCKET_ID, 0)
|| '-' || nvl(ods.BUCKET_NAME, 'NA')
|| '-' || nvl(ods.BUCKET_SEQUENCE_NUM, 0)
);

commit;
end;


UPDATE bec_etl_ctrl.batch_dw_info
SET
    last_refresh_date = getdate()
WHERE
    dw_table_name = 'dim_ar_aging_buckets' and batch_name = 'ar';

COMMIT;