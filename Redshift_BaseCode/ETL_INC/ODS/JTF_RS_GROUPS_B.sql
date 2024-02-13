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

delete from bec_ods.JTF_RS_GROUPS_B
where (nvl(GROUP_ID,0)) in (
select nvl(stg.GROUP_ID,0) as GROUP_ID from bec_ods.JTF_RS_GROUPS_B ods, bec_ods_stg.JTF_RS_GROUPS_B stg
where nvl(ods.GROUP_ID,0) = nvl(stg.GROUP_ID,0) 
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.JTF_RS_GROUPS_B
       (	
		GROUP_ID,  
		GROUP_NUMBER,  
		CREATED_BY,  
		CREATION_DATE, 
		LAST_UPDATED_BY,  
		LAST_UPDATE_DATE, 
		LAST_UPDATE_LOGIN,  
		EXCLUSIVE_FLAG,  
		START_DATE_ACTIVE, 
		END_DATE_ACTIVE, 
		ACCOUNTING_CODE,  
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
		ATTRIBUTE_CATEGORY,  
		EMAIL_ADDRESS,  
		OBJECT_VERSION_NUMBER,  
		SECURITY_GROUP_ID,  
		TIME_ZONE,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_ID,
		kca_seq_date)	
(
	select
		GROUP_ID,  
		GROUP_NUMBER,  
		CREATED_BY,  
		CREATION_DATE, 
		LAST_UPDATED_BY,  
		LAST_UPDATE_DATE, 
		LAST_UPDATE_LOGIN,  
		EXCLUSIVE_FLAG,  
		START_DATE_ACTIVE, 
		END_DATE_ACTIVE, 
		ACCOUNTING_CODE,  
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
		ATTRIBUTE_CATEGORY,  
		EMAIL_ADDRESS,  
		OBJECT_VERSION_NUMBER,  
		SECURITY_GROUP_ID,  
		TIME_ZONE,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.JTF_RS_GROUPS_B
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(GROUP_ID,0),kca_seq_ID) in 
	(select nvl(GROUP_ID,0) as GROUP_ID,max(kca_seq_ID) as kca_seq_ID from bec_ods_stg.JTF_RS_GROUPS_B 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(GROUP_ID,0))
);

commit;

-- Soft delete
update bec_ods.JTF_RS_GROUPS_B set IS_DELETED_FLG = 'N';
commit;
update bec_ods.JTF_RS_GROUPS_B set IS_DELETED_FLG = 'Y'
where (GROUP_ID)  in
(
select GROUP_ID from bec_raw_dl_ext.JTF_RS_GROUPS_B
where (GROUP_ID,KCA_SEQ_ID)
in 
(
select GROUP_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.JTF_RS_GROUPS_B
group by GROUP_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'jtf_rs_groups_b';

commit;