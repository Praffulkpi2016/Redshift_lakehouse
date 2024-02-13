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

delete from bec_ods.oe_workflow_assignments
where (nvl(ASSIGNMENT_ID,0)) in (
select nvl(stg.ASSIGNMENT_ID,0) as ASSIGNMENT_ID from bec_ods.oe_workflow_assignments ods, bec_ods_stg.oe_workflow_assignments stg
where nvl(ods.ASSIGNMENT_ID,0) = nvl(stg.ASSIGNMENT_ID,0) and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.oe_workflow_assignments
       (	
		ORDER_TYPE_ID,
		LINE_TYPE_ID,
		PROCESS_NAME,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_LOGIN,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		REQUEST_ID,
		CONTEXT,
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
		ITEM_TYPE_CODE,
		ASSIGNMENT_ID,
		START_DATE_ACTIVE,
		END_DATE_ACTIVE,
		WF_ITEM_TYPE,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id
		,kca_seq_date)	
(
	select
		ORDER_TYPE_ID,
		LINE_TYPE_ID,
		PROCESS_NAME,
		CREATION_DATE,
		CREATED_BY,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		LAST_UPDATE_LOGIN,
		PROGRAM_APPLICATION_ID,
		PROGRAM_ID,
		REQUEST_ID,
		CONTEXT,
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
		ITEM_TYPE_CODE,
		ASSIGNMENT_ID,
		START_DATE_ACTIVE,
		END_DATE_ACTIVE,
		WF_ITEM_TYPE, 
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.oe_workflow_assignments
	where kca_operation in ('INSERT','UPDATE') 
	and (nvl(ASSIGNMENT_ID,0),kca_seq_id) in 
	(select nvl(ASSIGNMENT_ID,0) as ASSIGNMENT_ID,max(kca_seq_id) from bec_ods_stg.oe_workflow_assignments 
     where kca_operation in ('INSERT','UPDATE')
     group by nvl(ASSIGNMENT_ID,0))
);

commit;


-- Soft delete
update bec_ods.oe_workflow_assignments set IS_DELETED_FLG = 'N';
commit;
update bec_ods.oe_workflow_assignments set IS_DELETED_FLG = 'Y'
where (ASSIGNMENT_ID)  in
(
select ASSIGNMENT_ID from bec_raw_dl_ext.oe_workflow_assignments
where (ASSIGNMENT_ID,KCA_SEQ_ID)
in 
(
select ASSIGNMENT_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.oe_workflow_assignments
group by ASSIGNMENT_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'oe_workflow_assignments';

commit;