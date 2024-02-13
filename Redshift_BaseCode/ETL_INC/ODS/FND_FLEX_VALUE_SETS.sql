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

delete from bec_ods.fnd_flex_value_sets
where FLEX_VALUE_SET_ID in (
select stg.FLEX_VALUE_SET_ID from bec_ods.fnd_flex_value_sets ods, bec_ods_stg.fnd_flex_value_sets stg
where ods.FLEX_VALUE_SET_ID = stg.FLEX_VALUE_SET_ID  and stg.kca_operation IN ('INSERT','UPDATE') );

commit;

 -- Insert records
insert into bec_ods.fnd_flex_value_sets
( FLEX_VALUE_SET_ID,
FLEX_VALUE_SET_NAME,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
VALIDATION_TYPE,
PROTECTED_FLAG,
SECURITY_ENABLED_FLAG,
LONGLIST_FLAG,
FORMAT_TYPE,
MAXIMUM_SIZE,
ALPHANUMERIC_ALLOWED_FLAG,
UPPERCASE_ONLY_FLAG,
NUMERIC_MODE_ENABLED_FLAG,
DESCRIPTION,
DEPENDANT_DEFAULT_VALUE,
DEPENDANT_DEFAULT_MEANING,
PARENT_FLEX_VALUE_SET_ID,
MINIMUM_VALUE,
MAXIMUM_VALUE,
NUMBER_PRECISION 
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION,
IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date )
(
select
   FLEX_VALUE_SET_ID,
FLEX_VALUE_SET_NAME,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
VALIDATION_TYPE,
PROTECTED_FLAG,
SECURITY_ENABLED_FLAG,
LONGLIST_FLAG,
FORMAT_TYPE,
MAXIMUM_SIZE,
ALPHANUMERIC_ALLOWED_FLAG,
UPPERCASE_ONLY_FLAG,
NUMERIC_MODE_ENABLED_FLAG,
DESCRIPTION,
DEPENDANT_DEFAULT_VALUE,
DEPENDANT_DEFAULT_MEANING,
PARENT_FLEX_VALUE_SET_ID,
MINIMUM_VALUE,
MAXIMUM_VALUE,
NUMBER_PRECISION 
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION,
'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date 
from bec_ods_stg.fnd_flex_value_sets
where kca_operation IN ('INSERT','UPDATE') and (FLEX_VALUE_SET_ID,kca_seq_id) in (select FLEX_VALUE_SET_ID,max(kca_seq_id) from bec_ods_stg.fnd_flex_value_sets 
where kca_operation IN ('INSERT','UPDATE')
group by FLEX_VALUE_SET_ID)
); 
commit; 

-- Soft delete
update bec_ods.fnd_flex_value_sets set IS_DELETED_FLG = 'N';
commit;
update bec_ods.fnd_flex_value_sets set IS_DELETED_FLG = 'Y'
where (FLEX_VALUE_SET_ID)  in
(
select FLEX_VALUE_SET_ID from bec_raw_dl_ext.fnd_flex_value_sets
where (FLEX_VALUE_SET_ID,KCA_SEQ_ID)
in 
(
select FLEX_VALUE_SET_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.fnd_flex_value_sets
group by FLEX_VALUE_SET_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;


update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='fnd_flex_value_sets';
commit;
