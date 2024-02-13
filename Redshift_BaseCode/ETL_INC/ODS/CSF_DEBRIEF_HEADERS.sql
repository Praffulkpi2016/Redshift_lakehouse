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

delete from bec_ods.CSF_DEBRIEF_HEADERS
where (nvl(DEBRIEF_HEADER_ID,0)) in (
select nvl(stg.DEBRIEF_HEADER_ID,0) as DEBRIEF_HEADER_ID from bec_ods.CSF_DEBRIEF_HEADERS ods, bec_ods_stg.CSF_DEBRIEF_HEADERS stg
where nvl(ods.DEBRIEF_HEADER_ID,0) = nvl(stg.DEBRIEF_HEADER_ID,0) 
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.CSF_DEBRIEF_HEADERS
       (	
		DEBRIEF_HEADER_ID, 
		DEBRIEF_NUMBER, 
		DEBRIEF_DATE, 
		DEBRIEF_STATUS_ID, 
		TASK_ASSIGNMENT_ID, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATED_BY, 
		LAST_UPDATE_DATE, 
		LAST_UPDATE_LOGIN, 
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
		SECURITY_GROUP_ID, 
		STATISTICS_UPDATED, 
		PROCESSED_FLAG, 
		TRAVEL_START_TIME, 
		TRAVEL_END_TIME, 
		OBJECT_VERSION_NUMBER, 
		TRAVEL_DISTANCE_IN_KM,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_ID,
		kca_seq_date)	
(
	select
		DEBRIEF_HEADER_ID, 
		DEBRIEF_NUMBER, 
		DEBRIEF_DATE, 
		DEBRIEF_STATUS_ID, 
		TASK_ASSIGNMENT_ID, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATED_BY, 
		LAST_UPDATE_DATE, 
		LAST_UPDATE_LOGIN, 
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
		SECURITY_GROUP_ID, 
		STATISTICS_UPDATED, 
		PROCESSED_FLAG, 
		TRAVEL_START_TIME, 
		TRAVEL_END_TIME, 
		OBJECT_VERSION_NUMBER, 
		TRAVEL_DISTANCE_IN_KM,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.CSF_DEBRIEF_HEADERS
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(DEBRIEF_HEADER_ID,0),kca_seq_ID) in 
	(select nvl(DEBRIEF_HEADER_ID,0) as DEBRIEF_HEADER_ID,max(kca_seq_ID) as kca_seq_ID from bec_ods_stg.CSF_DEBRIEF_HEADERS 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(DEBRIEF_HEADER_ID,0))
);

commit;

-- Soft delete
update bec_ods.CSF_DEBRIEF_HEADERS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.CSF_DEBRIEF_HEADERS set IS_DELETED_FLG = 'Y'
where (DEBRIEF_HEADER_ID )  in
(
select DEBRIEF_HEADER_ID  from bec_raw_dl_ext.CSF_DEBRIEF_HEADERS
where (DEBRIEF_HEADER_ID ,KCA_SEQ_ID)
in 
(
select DEBRIEF_HEADER_ID ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.CSF_DEBRIEF_HEADERS
group by DEBRIEF_HEADER_ID 
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'csf_debrief_headers';

commit;