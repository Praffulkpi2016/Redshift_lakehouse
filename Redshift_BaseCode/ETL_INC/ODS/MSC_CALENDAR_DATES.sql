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

delete from bec_ods.MSC_CALENDAR_DATES
where ( SR_INSTANCE_ID,CALENDAR_DATE,CALENDAR_CODE,EXCEPTION_SET_ID )
 in (
    select stg.SR_INSTANCE_ID,stg.CALENDAR_DATE, stg.CALENDAR_CODE, stg.EXCEPTION_SET_ID     
    from bec_ods.MSC_CALENDAR_DATES ods, bec_ods_stg.MSC_CALENDAR_DATES stg
    where ods.SR_INSTANCE_ID = stg.SR_INSTANCE_ID
        and ods.CALENDAR_DATE = stg.CALENDAR_DATE
        and ods.CALENDAR_CODE = stg.CALENDAR_CODE
        and ods.EXCEPTION_SET_ID = stg.EXCEPTION_SET_ID
        and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;


-- Insert records

insert into	bec_ods.MSC_CALENDAR_DATES
    (
 CALENDAR_DATE
,      CALENDAR_CODE
,      SEQ_NUM
,      NEXT_SEQ_NUM
,      PRIOR_SEQ_NUM
,      NEXT_DATE
,      PRIOR_DATE
,      CALENDAR_START_DATE
,      CALENDAR_END_DATE
,      DESCRIPTION
,      EXCEPTION_SET_ID
,      SR_INSTANCE_ID
,      REFRESH_NUMBER
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
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
,      DELETED_FLAG
,	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(select
	 CALENDAR_DATE
,      CALENDAR_CODE
,      SEQ_NUM
,      NEXT_SEQ_NUM
,      PRIOR_SEQ_NUM
,      NEXT_DATE
,      PRIOR_DATE
,      CALENDAR_START_DATE
,      CALENDAR_END_DATE
,      DESCRIPTION
,      EXCEPTION_SET_ID
,      SR_INSTANCE_ID
,      REFRESH_NUMBER
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
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
,      DELETED_FLAG
,	KCA_OPERATION,
	'N' AS IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from
	bec_ods_stg.MSC_CALENDAR_DATES
where kca_operation IN ('INSERT','UPDATE') 
	and (SR_INSTANCE_ID,CALENDAR_DATE,CALENDAR_CODE,EXCEPTION_SET_ID,KCA_SEQ_ID) in 
	( select SR_INSTANCE_ID,CALENDAR_DATE,CALENDAR_CODE,EXCEPTION_SET_ID,max(KCA_SEQ_ID) from bec_ods_stg.MSC_CALENDAR_DATES 
     where kca_operation IN ('INSERT','UPDATE')
     group by SR_INSTANCE_ID,CALENDAR_DATE,CALENDAR_CODE,EXCEPTION_SET_ID )
);

commit;

-- Soft delete
update bec_ods.MSC_CALENDAR_DATES set IS_DELETED_FLG = 'N';
commit;
update bec_ods.MSC_CALENDAR_DATES set IS_DELETED_FLG = 'Y'
where (SR_INSTANCE_ID,CALENDAR_DATE,CALENDAR_CODE,EXCEPTION_SET_ID)  in
(
select SR_INSTANCE_ID,CALENDAR_DATE,CALENDAR_CODE,EXCEPTION_SET_ID from bec_raw_dl_ext.MSC_CALENDAR_DATES
where (SR_INSTANCE_ID,CALENDAR_DATE,CALENDAR_CODE,EXCEPTION_SET_ID,KCA_SEQ_ID)
in 
(
select SR_INSTANCE_ID,CALENDAR_DATE,CALENDAR_CODE,EXCEPTION_SET_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.MSC_CALENDAR_DATES
group by SR_INSTANCE_ID,CALENDAR_DATE,CALENDAR_CODE,EXCEPTION_SET_ID
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
	ods_table_name = 'msc_calendar_dates';
 