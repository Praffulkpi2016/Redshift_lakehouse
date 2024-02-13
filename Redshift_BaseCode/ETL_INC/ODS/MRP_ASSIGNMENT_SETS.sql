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

delete from bec_ods.MRP_ASSIGNMENT_SETS
where ASSIGNMENT_SET_ID in (
select stg.ASSIGNMENT_SET_ID from bec_ods.MRP_ASSIGNMENT_SETS ods, bec_ods_stg.MRP_ASSIGNMENT_SETS stg
where ods.ASSIGNMENT_SET_ID = stg.ASSIGNMENT_SET_ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.MRP_ASSIGNMENT_SETS
       (
	   ASSIGNMENT_SET_ID
,      ASSIGNMENT_SET_NAME
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      DESCRIPTION
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
 ,       KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
	ASSIGNMENT_SET_ID
,      ASSIGNMENT_SET_NAME
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
,      DESCRIPTION
,      ATTRIBUTE_CATEGORY
,      ATTRIBUTE1
,      ATTRIBUTE2
,      ATTRIBUTE3
,      ATTRIBUTE4
,      ATTRIBUTE5
,      ATTRIBUTE6
,      ATTRIBUTE7
,      ATTRIBUTE8
,      ATTRIBUTE9
,      ATTRIBUTE10
,      ATTRIBUTE11
,      ATTRIBUTE12
,      ATTRIBUTE13
,      ATTRIBUTE14
,      ATTRIBUTE15
 ,       KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.MRP_ASSIGNMENT_SETS
	where kca_operation IN ('INSERT','UPDATE') 
	and (ASSIGNMENT_SET_ID,kca_seq_id) in 
	(select ASSIGNMENT_SET_ID,max(kca_seq_id) from bec_ods_stg.MRP_ASSIGNMENT_SETS 
     where kca_operation IN ('INSERT','UPDATE')
     group by ASSIGNMENT_SET_ID)
);

commit;

-- Soft delete
update bec_ods.MRP_ASSIGNMENT_SETS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MRP_ASSIGNMENT_SETS set IS_DELETED_FLG = 'Y'
where (ASSIGNMENT_SET_ID)  in
(
select ASSIGNMENT_SET_ID from bec_raw_dl_ext.MRP_ASSIGNMENT_SETS
where (ASSIGNMENT_SET_ID,KCA_SEQ_ID)
in 
(
select ASSIGNMENT_SET_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MRP_ASSIGNMENT_SETS
group by ASSIGNMENT_SET_ID
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'mrp_assignment_sets'; 

commit;