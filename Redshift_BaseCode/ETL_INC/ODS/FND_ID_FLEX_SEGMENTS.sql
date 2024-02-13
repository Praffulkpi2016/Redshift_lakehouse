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

delete from bec_ods.fnd_id_flex_segments
where (APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,APPLICATION_COLUMN_NAME) in (
select stg.APPLICATION_ID,stg.ID_FLEX_CODE,stg.ID_FLEX_NUM,stg.APPLICATION_COLUMN_NAME from bec_ods.fnd_id_flex_segments ods, bec_ods_stg.fnd_id_flex_segments stg
where ods.APPLICATION_ID = stg.APPLICATION_ID and ods.ID_FLEX_CODE = stg.ID_FLEX_CODE and ods.ID_FLEX_NUM = stg.ID_FLEX_NUM
and ods.APPLICATION_COLUMN_NAME = stg.APPLICATION_COLUMN_NAME and stg.kca_operation IN ('INSERT','UPDATE') );

commit;
 -- Insert records

insert into bec_ods.fnd_id_flex_segments
(
APPLICATION_ID
,ID_FLEX_CODE
,ID_FLEX_NUM
,APPLICATION_COLUMN_NAME
,SEGMENT_NAME
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,SEGMENT_NUM
,APPLICATION_COLUMN_INDEX_FLAG
,ENABLED_FLAG
,REQUIRED_FLAG
,DISPLAY_FLAG
,DISPLAY_SIZE
,SECURITY_ENABLED_FLAG
,MAXIMUM_DESCRIPTION_LEN
,CONCATENATION_DESCRIPTION_LEN
,FLEX_VALUE_SET_ID
,RANGE_CODE
,DEFAULT_TYPE
,DEFAULT_VALUE
,RUNTIME_PROPERTY_FUNCTION 
,ADDITIONAL_WHERE_CLAUSE
,SEGMENT_INSERT_FLAG
,SEGMENT_UPDATE_FLAG
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION 
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date
)
(
select 
APPLICATION_ID
,ID_FLEX_CODE
,ID_FLEX_NUM
,APPLICATION_COLUMN_NAME
,SEGMENT_NAME
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,SEGMENT_NUM
,APPLICATION_COLUMN_INDEX_FLAG
,ENABLED_FLAG
,REQUIRED_FLAG
,DISPLAY_FLAG
,DISPLAY_SIZE
,SECURITY_ENABLED_FLAG
,MAXIMUM_DESCRIPTION_LEN
,CONCATENATION_DESCRIPTION_LEN
,FLEX_VALUE_SET_ID
,RANGE_CODE
,DEFAULT_TYPE
,DEFAULT_VALUE
,RUNTIME_PROPERTY_FUNCTION 
,ADDITIONAL_WHERE_CLAUSE
,SEGMENT_INSERT_FLAG
,SEGMENT_UPDATE_FLAG
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION 
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from bec_ods_stg.fnd_id_flex_segments
where kca_operation IN ('INSERT','UPDATE') and (APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,APPLICATION_COLUMN_NAME,kca_seq_id) in (select APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,APPLICATION_COLUMN_NAME,max(kca_seq_id) from bec_ods_stg.fnd_id_flex_segments 
where kca_operation IN ('INSERT','UPDATE')
group by APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,APPLICATION_COLUMN_NAME));

commit;
--Soft Delete
update bec_ods.fnd_id_flex_segments set IS_DELETED_FLG = 'N';
commit;
update bec_ods.fnd_id_flex_segments set IS_DELETED_FLG = 'Y'
where (APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,APPLICATION_COLUMN_NAME)  in
(
select APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,APPLICATION_COLUMN_NAME from bec_raw_dl_ext.fnd_id_flex_segments
where (APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,APPLICATION_COLUMN_NAME,KCA_SEQ_ID)
in 
(
select APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,APPLICATION_COLUMN_NAME,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.fnd_id_flex_segments
group by APPLICATION_ID,ID_FLEX_CODE,ID_FLEX_NUM,APPLICATION_COLUMN_NAME
) 
and kca_operation= 'DELETE'
);
commit;
end;


update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='fnd_id_flex_segments';
commit;
