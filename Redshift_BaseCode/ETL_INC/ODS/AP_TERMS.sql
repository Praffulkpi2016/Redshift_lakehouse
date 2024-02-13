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

delete from bec_ods.AP_TERMS
where (term_id,language) in (
select stg.term_id,stg.language from bec_ods.AP_TERMS ods, bec_ods_stg.AP_TERMS stg
where ods.term_id = stg.term_id and ods.language = stg.language and stg.kca_operation IN ('INSERT','UPDATE') );

commit;

insert into bec_ods.ap_terms
(
TERM_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,ENABLED_FLAG
,DUE_CUTOFF_DAY
,TYPE
,START_DATE_ACTIVE
,END_DATE_ACTIVE
,RANK
,ATTRIBUTE_CATEGORY
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,NAME
,DESCRIPTION,
KCA_OPERATION,
IS_DELETED_FLG
,KCA_SEQ_ID
,kca_seq_date
)
(
select
TERM_ID
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,CREATION_DATE
,CREATED_BY
,LAST_UPDATE_LOGIN
,ENABLED_FLAG
,DUE_CUTOFF_DAY
,TYPE
,START_DATE_ACTIVE
,END_DATE_ACTIVE
,RANK
,ATTRIBUTE_CATEGORY
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,ATTRIBUTE6
,ATTRIBUTE7
,ATTRIBUTE8
,ATTRIBUTE9
,ATTRIBUTE10
,ATTRIBUTE11
,ATTRIBUTE12
,ATTRIBUTE13
,ATTRIBUTE14
,ATTRIBUTE15
,NAME
,DESCRIPTION,
KCA_OPERATION,
'N' AS IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
from bec_ods_stg.AP_TERMS
where kca_operation IN ('INSERT','UPDATE') and (TERM_ID,language,kca_seq_id) in (select TERM_ID,language,max(kca_seq_id) from bec_ods_stg.AP_TERMS 
where kca_operation IN ('INSERT','UPDATE')
group by TERM_ID,language)
);

commit;

-- Soft delete
update bec_ods.AP_TERMS set IS_DELETED_FLG = 'N';
commit;
update bec_ods.AP_TERMS set IS_DELETED_FLG = 'Y'
where (TERM_ID,language)  in
(
select TERM_ID,language from bec_raw_dl_ext.AP_TERMS_TL
where (TERM_ID,language,KCA_SEQ_ID)
in 
(
select TERM_ID,language,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.AP_TERMS_TL
group by TERM_ID,language
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='ap_terms';

commit;