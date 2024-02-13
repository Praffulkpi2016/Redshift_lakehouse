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

delete from bec_ods.CST_COST_GROUPS
where (COST_GROUP_ID) in (
select stg.COST_GROUP_ID from bec_ods.CST_COST_GROUPS ods, bec_ods_stg.CST_COST_GROUPS stg
where ods.COST_GROUP_ID = stg.COST_GROUP_ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.CST_COST_GROUPS
       (COST_GROUP_ID,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_LOGIN,
		REQUEST_ID,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		PROGRAM_UPDATE_DATE,
		ORGANIZATION_ID,
		COST_GROUP,
		DESCRIPTION,
		DISABLE_DATE,
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
		LEGAL_ENTITY,
		COST_GROUP_TYPE,
		ZD_EDITION_NAME,
		ZD_SYNC,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		COST_GROUP_ID,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_LOGIN,
		REQUEST_ID,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		PROGRAM_UPDATE_DATE,
		ORGANIZATION_ID,
		COST_GROUP,
		DESCRIPTION,
		DISABLE_DATE,
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
		LEGAL_ENTITY,
		COST_GROUP_TYPE,
		ZD_EDITION_NAME,
		ZD_SYNC,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.CST_COST_GROUPS
	where kca_operation IN ('INSERT','UPDATE') 
	and (COST_GROUP_ID,kca_seq_id) in 
	(select COST_GROUP_ID,max(kca_seq_id) from bec_ods_stg.CST_COST_GROUPS 
     where kca_operation IN ('INSERT','UPDATE')
     group by COST_GROUP_ID)
);

commit;

-- Soft delete
update bec_ods.CST_COST_GROUPS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.CST_COST_GROUPS set IS_DELETED_FLG = 'Y'
where (COST_GROUP_ID)  in
(
select COST_GROUP_ID from bec_raw_dl_ext.CST_COST_GROUPS
where (COST_GROUP_ID,KCA_SEQ_ID)
in 
(
select COST_GROUP_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.CST_COST_GROUPS
group by COST_GROUP_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'cst_cost_groups';

commit;