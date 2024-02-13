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

delete from bec_ods.WIP_SCHEDULE_GROUPS
where SCHEDULE_GROUP_ID in (
select stg.SCHEDULE_GROUP_ID from bec_ods.WIP_SCHEDULE_GROUPS ods, bec_ods_stg.WIP_SCHEDULE_GROUPS stg
where ods.SCHEDULE_GROUP_ID = stg.SCHEDULE_GROUP_ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.WIP_SCHEDULE_GROUPS
       (
		schedule_group_name,
		schedule_group_id,
		organization_id,
		description,
		inactive_on,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		program_application_id,
		program_id,
		program_update_date,
		attribute_category,
		request_id,
		attribute1,
		attribute2,
		attribute3,
		attribute4,
		attribute5,
		attribute6,
		attribute7,
		attribute8,
		attribute9,
		attribute10,
		attribute11,
		attribute12,
		attribute13,
		attribute14,
		attribute15,
		kca_operation,
		is_deleted_flg,
		kca_seq_id
		,kca_seq_date
)
(
	SELECT
		schedule_group_name,
		schedule_group_id,
		organization_id,
		description,
		inactive_on,
		last_update_date,
		last_updated_by,
		creation_date,
		created_by,
		last_update_login,
		program_application_id,
		program_id,
		program_update_date,
		attribute_category,
		request_id,
		attribute1,
		attribute2,
		attribute3,
		attribute4,
		attribute5,
		attribute6,
		attribute7,
		attribute8,
		attribute9,
		attribute10,
		attribute11,
		attribute12,
		attribute13,
		attribute14,
		attribute15,
		kca_operation,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.WIP_SCHEDULE_GROUPS
	where kca_operation in ('INSERT','UPDATE') 
	and (SCHEDULE_GROUP_ID,kca_seq_id) in 
	(select SCHEDULE_GROUP_ID,max(kca_seq_id) from bec_ods_stg.WIP_SCHEDULE_GROUPS 
     where kca_operation in ('INSERT','UPDATE')
     group by SCHEDULE_GROUP_ID)
);

commit;



-- Soft delete
update bec_ods.WIP_SCHEDULE_GROUPS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.WIP_SCHEDULE_GROUPS set IS_DELETED_FLG = 'Y'
where (SCHEDULE_GROUP_ID)  in
(
select SCHEDULE_GROUP_ID from bec_raw_dl_ext.WIP_SCHEDULE_GROUPS
where (SCHEDULE_GROUP_ID,KCA_SEQ_ID)
in 
(
select SCHEDULE_GROUP_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.WIP_SCHEDULE_GROUPS
group by SCHEDULE_GROUP_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'wip_schedule_groups';

commit;