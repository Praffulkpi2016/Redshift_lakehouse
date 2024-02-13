/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
 
begin;
-- Delete Records

delete
from
	bec_ods.ORG_ACCT_PERIODS
where
	(
	nvl(ACCT_PERIOD_ID, 0) ,nvl(ORGANIZATION_ID, 0) 
	) in 
	(
	select
		NVL(stg.ACCT_PERIOD_ID, 0) as ACCT_PERIOD_ID ,
		NVL(stg.ORGANIZATION_ID, 0) as ORGANIZATION_ID 
	from
		bec_ods.ORG_ACCT_PERIODS ods,
		bec_ods_stg.ORG_ACCT_PERIODS stg
	where
		    NVL(ods.ACCT_PERIOD_ID, 0) = NVL(stg.ACCT_PERIOD_ID, 0) AND 
			NVL(ods.ORGANIZATION_ID, 0) = NVL(stg.ORGANIZATION_ID, 0)
					and stg.kca_operation in ('INSERT', 'UPDATE')
);

commit;
-- Insert records

insert
	into
	bec_ods.ORG_ACCT_PERIODS (
		acct_period_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	period_set_name,
	period_year,
	period_num,
	period_name,
	description,
	period_start_date,
	schedule_close_date,
	period_close_date,
	open_flag,
	global_attribute_category,
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	global_attribute7,
	global_attribute8,
	global_attribute9,
	global_attribute10,
	global_attribute11,
	global_attribute12,
	global_attribute13,
	global_attribute14,
	global_attribute15,
	global_attribute16,
	global_attribute17,
	global_attribute18,
	global_attribute19,
	global_attribute20,
	summarized_flag,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
	select
	acct_period_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	period_set_name,
	period_year,
	period_num,
	period_name,
	description,
	period_start_date,
	schedule_close_date,
	period_close_date,
	open_flag,
	global_attribute_category,
	global_attribute1,
	global_attribute2,
	global_attribute3,
	global_attribute4,
	global_attribute5,
	global_attribute6,
	global_attribute7,
	global_attribute8,
	global_attribute9,
	global_attribute10,
	global_attribute11,
	global_attribute12,
	global_attribute13,
	global_attribute14,
	global_attribute15,
	global_attribute16,
	global_attribute17,
	global_attribute18,
	global_attribute19,
	global_attribute20,
	summarized_flag,
		kca_operation,
		'N' as IS_DELETED_FLG,
		cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID,
		kca_seq_date
	from
		bec_ods_stg.ORG_ACCT_PERIODS
	where
		kca_operation IN ('INSERT','UPDATE')
		and (
		nvl(ACCT_PERIOD_ID, 0) , nvl(ORGANIZATION_ID, 0),
		KCA_SEQ_ID
		) in 
	(
		select
			nvl(ACCT_PERIOD_ID, 0) as ACCT_PERIOD_ID ,
			nvl(ORGANIZATION_ID, 0) as ORGANIZATION_ID ,
			max(KCA_SEQ_ID)
		from
			bec_ods_stg.ORG_ACCT_PERIODS
		where
			kca_operation IN ('INSERT','UPDATE')
		group by
			ACCT_PERIOD_ID ,ORGANIZATION_ID
			)	
	);

commit;
-- Soft delete
update bec_ods.ORG_ACCT_PERIODS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.ORG_ACCT_PERIODS set IS_DELETED_FLG = 'Y'
where (nvl(ACCT_PERIOD_ID, 0),nvl(ORGANIZATION_ID, 0))  in
(
select nvl(ACCT_PERIOD_ID, 0),nvl(ORGANIZATION_ID, 0) from bec_raw_dl_ext.ORG_ACCT_PERIODS
where (nvl(ACCT_PERIOD_ID, 0),nvl(ORGANIZATION_ID, 0),KCA_SEQ_ID)
in 
(
select nvl(ACCT_PERIOD_ID, 0),nvl(ORGANIZATION_ID, 0),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.ORG_ACCT_PERIODS
group by nvl(ACCT_PERIOD_ID, 0),nvl(ORGANIZATION_ID, 0)
) 
and kca_operation= 'DELETE'
);
commit;
end;

update
	bec_etl_ctrl.batch_ods_info
set
	last_refresh_date = getdate()
where
	ods_table_name = 'org_acct_periods';

commit;