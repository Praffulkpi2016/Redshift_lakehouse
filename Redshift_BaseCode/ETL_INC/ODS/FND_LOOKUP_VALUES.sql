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

delete from bec_ods.fnd_lookup_values
where (LOOKUP_TYPE, LANGUAGE, SECURITY_GROUP_ID, LOOKUP_CODE, VIEW_APPLICATION_ID) in (
select stg.LOOKUP_TYPE,stg.LANGUAGE,stg.SECURITY_GROUP_ID,stg.LOOKUP_CODE,stg.VIEW_APPLICATION_ID from bec_ods.fnd_lookup_values ods, bec_ods_stg.fnd_lookup_values stg
where ods.LOOKUP_TYPE = stg.LOOKUP_TYPE and ods.LANGUAGE = stg.LANGUAGE and ods.SECURITY_GROUP_ID = stg.SECURITY_GROUP_ID 
and ods.LOOKUP_CODE = stg.LOOKUP_CODE and ods.VIEW_APPLICATION_ID = stg.VIEW_APPLICATION_ID  and stg.kca_operation IN ('INSERT','UPDATE') );

commit;

insert into bec_ods.fnd_lookup_values
(
LOOKUP_TYPE,
LANGUAGE,
LOOKUP_CODE,
MEANING,
DESCRIPTION,
ENABLED_FLAG,
START_DATE_ACTIVE,
END_DATE_ACTIVE,
CREATED_BY,
CREATION_DATE,
LAST_UPDATED_BY,
LAST_UPDATE_LOGIN,
LAST_UPDATE_DATE,
SOURCE_LANG,
SECURITY_GROUP_ID,
VIEW_APPLICATION_ID,
TERRITORY_CODE,
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
"TAG",
LEAF_NODE
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date

)
(
select LOOKUP_TYPE,
LANGUAGE,
LOOKUP_CODE,
MEANING,
DESCRIPTION,
ENABLED_FLAG,
START_DATE_ACTIVE,
END_DATE_ACTIVE,
CREATED_BY,
CREATION_DATE,
LAST_UPDATED_BY,
LAST_UPDATE_LOGIN,
LAST_UPDATE_DATE,
SOURCE_LANG,
SECURITY_GROUP_ID,
VIEW_APPLICATION_ID,
TERRITORY_CODE,
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
"TAG",
LEAF_NODE
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from bec_ods_stg.fnd_lookup_values
where kca_operation IN ('INSERT','UPDATE') and (LOOKUP_TYPE, LANGUAGE, SECURITY_GROUP_ID, LOOKUP_CODE, VIEW_APPLICATION_ID,kca_seq_id) in (select LOOKUP_TYPE, LANGUAGE, SECURITY_GROUP_ID, LOOKUP_CODE, VIEW_APPLICATION_ID,max(kca_seq_id) from bec_ods_stg.fnd_lookup_values 
where kca_operation IN ('INSERT','UPDATE')
group by LOOKUP_TYPE, LANGUAGE, SECURITY_GROUP_ID, LOOKUP_CODE, VIEW_APPLICATION_ID,last_update_date)
);

commit;

-- Soft delete
update bec_ods.fnd_lookup_values set IS_DELETED_FLG = 'N';
commit;
update bec_ods.fnd_lookup_values set IS_DELETED_FLG = 'Y'
where (LOOKUP_TYPE, LANGUAGE, SECURITY_GROUP_ID, LOOKUP_CODE, VIEW_APPLICATION_ID)  in
(
select LOOKUP_TYPE, LANGUAGE, SECURITY_GROUP_ID, LOOKUP_CODE, VIEW_APPLICATION_ID from bec_raw_dl_ext.fnd_lookup_values
where (LOOKUP_TYPE, LANGUAGE, SECURITY_GROUP_ID, LOOKUP_CODE, VIEW_APPLICATION_ID,KCA_SEQ_ID)
in 
(
select LOOKUP_TYPE, LANGUAGE, SECURITY_GROUP_ID, LOOKUP_CODE, VIEW_APPLICATION_ID,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.fnd_lookup_values
group by LOOKUP_TYPE, LANGUAGE, SECURITY_GROUP_ID, LOOKUP_CODE, VIEW_APPLICATION_ID
) 
and kca_operation= 'DELETE'
);
commit;
end;


update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='fnd_lookup_values';
commit;
