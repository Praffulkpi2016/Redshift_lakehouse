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

delete from bec_ods.CSI_COUNTER_ASSOCIATIONS
where (nvl(INSTANCE_ASSOCIATION_ID,0)) in (
select nvl(stg.INSTANCE_ASSOCIATION_ID,0) as INSTANCE_ASSOCIATION_ID from bec_ods.CSI_COUNTER_ASSOCIATIONS ods, bec_ods_stg.CSI_COUNTER_ASSOCIATIONS stg
where nvl(ods.INSTANCE_ASSOCIATION_ID,0) = nvl(stg.INSTANCE_ASSOCIATION_ID,0)  
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.CSI_COUNTER_ASSOCIATIONS
       (	
		INSTANCE_ASSOCIATION_ID,
		SOURCE_OBJECT_CODE,
		SOURCE_OBJECT_ID,
		COUNTER_ID,
		START_DATE_ACTIVE,
		END_DATE_ACTIVE,
		SECURITY_GROUP_ID,
		OBJECT_VERSION_NUMBER,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_LOGIN,
		ATTRIBUTE_CATEGORY,
		ATTRIBUTE1,
		ATTRIBUTE2,
		ATTRIBUTE3,
		ATTRIBUTE4,
		ATTRIBUTE5,
		ATTRIBUTE6,
		ATTRIBUTE7,
		ATTRIBUTE8,
		ATTRIBUTE9,
		ATTRIBUTE10,
		ATTRIBUTE11,
		ATTRIBUTE12,
		ATTRIBUTE13,
		ATTRIBUTE14,
		ATTRIBUTE15,
		MIGRATED_FLAG,
		MAINT_ORGANIZATION_ID,
		PRIMARY_FAILURE_FLAG,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		INSTANCE_ASSOCIATION_ID,
		SOURCE_OBJECT_CODE,
		SOURCE_OBJECT_ID,
		COUNTER_ID,
		START_DATE_ACTIVE,
		END_DATE_ACTIVE,
		SECURITY_GROUP_ID,
		OBJECT_VERSION_NUMBER,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_LOGIN,
		ATTRIBUTE_CATEGORY,
		ATTRIBUTE1,
		ATTRIBUTE2,
		ATTRIBUTE3,
		ATTRIBUTE4,
		ATTRIBUTE5,
		ATTRIBUTE6,
		ATTRIBUTE7,
		ATTRIBUTE8,
		ATTRIBUTE9,
		ATTRIBUTE10,
		ATTRIBUTE11,
		ATTRIBUTE12,
		ATTRIBUTE13,
		ATTRIBUTE14,
		ATTRIBUTE15,
		MIGRATED_FLAG,
		MAINT_ORGANIZATION_ID,
		PRIMARY_FAILURE_FLAG,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.CSI_COUNTER_ASSOCIATIONS
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(INSTANCE_ASSOCIATION_ID,0),kca_seq_id) in 
	(select nvl(INSTANCE_ASSOCIATION_ID,0) as INSTANCE_ASSOCIATION_ID,max(kca_seq_id) as kca_seq_id from bec_ods_stg.CSI_COUNTER_ASSOCIATIONS 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(INSTANCE_ASSOCIATION_ID,0)
	 )
);

commit;

-- Soft delete
update bec_ods.CSI_COUNTER_ASSOCIATIONS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.CSI_COUNTER_ASSOCIATIONS set IS_DELETED_FLG = 'Y'
where (INSTANCE_ASSOCIATION_ID)  in
(
select INSTANCE_ASSOCIATION_ID from bec_raw_dl_ext.CSI_COUNTER_ASSOCIATIONS
where (INSTANCE_ASSOCIATION_ID,KCA_SEQ_ID)
in 
(
select INSTANCE_ASSOCIATION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.CSI_COUNTER_ASSOCIATIONS
group by INSTANCE_ASSOCIATION_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'csi_counter_associations';

commit;