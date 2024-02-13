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

delete from bec_ods.chv_bucket_patterns
where BUCKET_PATTERN_ID in (
select stg.BUCKET_PATTERN_ID from bec_ods.chv_bucket_patterns ods, bec_ods_stg.chv_bucket_patterns stg
where ods.BUCKET_PATTERN_ID = stg.BUCKET_PATTERN_ID and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.chv_bucket_patterns
       (
	   BUCKET_PATTERN_ID
,      BUCKET_PATTERN_NAME
,      NUMBER_DAILY_BUCKETS
,      NUMBER_WEEKLY_BUCKETS
,      NUMBER_MONTHLY_BUCKETS
,      NUMBER_QUARTERLY_BUCKETS
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      WEEK_START_DAY
,      DESCRIPTION
,      INACTIVE_DATE
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
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
 ,       KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		 BUCKET_PATTERN_ID
,      BUCKET_PATTERN_NAME
,      NUMBER_DAILY_BUCKETS
,      NUMBER_WEEKLY_BUCKETS
,      NUMBER_MONTHLY_BUCKETS
,      NUMBER_QUARTERLY_BUCKETS
,      LAST_UPDATE_DATE
,      LAST_UPDATED_BY
,      CREATION_DATE
,      CREATED_BY
,      WEEK_START_DAY
,      DESCRIPTION
,      INACTIVE_DATE
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
,      LAST_UPDATE_LOGIN
,      REQUEST_ID
,      PROGRAM_APPLICATION_ID
,      PROGRAM_ID
,      PROGRAM_UPDATE_DATE
 ,       KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.chv_bucket_patterns
	where kca_operation IN ('INSERT','UPDATE') 
	and (BUCKET_PATTERN_ID,kca_seq_id) in 
	(select BUCKET_PATTERN_ID,max(kca_seq_id) from bec_ods_stg.chv_bucket_patterns 
     where kca_operation IN ('INSERT','UPDATE')
     group by BUCKET_PATTERN_ID)
);

commit;

-- Soft delete
update bec_ods.chv_bucket_patterns set IS_DELETED_FLG = 'N';
commit;
update bec_ods.chv_bucket_patterns set IS_DELETED_FLG = 'Y'
where (BUCKET_PATTERN_ID )  in
(
select BUCKET_PATTERN_ID  from bec_raw_dl_ext.chv_bucket_patterns
where (BUCKET_PATTERN_ID ,KCA_SEQ_ID)
in 
(
select BUCKET_PATTERN_ID ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.chv_bucket_patterns
group by BUCKET_PATTERN_ID 
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'chv_bucket_patterns'; 

commit;