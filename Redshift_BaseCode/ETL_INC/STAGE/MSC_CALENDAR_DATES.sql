/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
BEGIN;

TRUNCATE TABLE bec_ods_stg.MSC_CALENDAR_DATES;

insert into	bec_ods_stg.MSC_CALENDAR_DATES
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
	,KCA_OPERATION,
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
	,KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
from
	bec_raw_dl_ext.MSC_CALENDAR_DATES
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and ( SR_INSTANCE_ID,CALENDAR_DATE,CALENDAR_CODE,EXCEPTION_SET_ID ,KCA_SEQ_ID ) in 
	(select SR_INSTANCE_ID,CALENDAR_DATE,CALENDAR_CODE,EXCEPTION_SET_ID ,max(KCA_SEQ_ID) from bec_raw_dl_ext.MSC_CALENDAR_DATES 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by SR_INSTANCE_ID,CALENDAR_DATE,CALENDAR_CODE,EXCEPTION_SET_ID )
     and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='msc_calendar_dates')
	 );
END;

