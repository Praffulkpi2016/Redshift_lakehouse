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

delete from bec_ods.gl_je_sources_tl
where (JE_SOURCE_NAME, LANGUAGE) in (
select stg.JE_SOURCE_NAME, stg.LANGUAGE from bec_ods.gl_je_sources_tl ods, bec_ods_stg.gl_je_sources_tl stg
where ods.JE_SOURCE_NAME = stg.JE_SOURCE_NAME and ods.LANGUAGE = stg.LANGUAGE  and stg.kca_operation IN ('INSERT','UPDATE') );

commit; 
-- Insert records
insert into bec_ods.gl_je_sources_tl
(
JE_SOURCE_NAME,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
OVERRIDE_EDITS_FLAG,
USER_JE_SOURCE_NAME,
JOURNAL_REFERENCE_FLAG,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
DESCRIPTION,
ATTRIBUTE1,
ATTRIBUTE2,
ATTRIBUTE3,
ATTRIBUTE4,
ATTRIBUTE5,
CONTEXT,
EFFECTIVE_DATE_RULE_CODE,
JOURNAL_APPROVAL_FLAG,
LANGUAGE,
SOURCE_LANG,
IMPORT_USING_KEY_FLAG,
JE_SOURCE_KEY
,KCA_OPERATION
,IS_DELETED_FLG
,KCA_SEQ_ID,
	kca_seq_date
)
(
select JE_SOURCE_NAME,
LAST_UPDATE_DATE,
LAST_UPDATED_BY,
OVERRIDE_EDITS_FLAG,
USER_JE_SOURCE_NAME,
JOURNAL_REFERENCE_FLAG,
CREATION_DATE,
CREATED_BY,
LAST_UPDATE_LOGIN,
DESCRIPTION,
ATTRIBUTE1,
ATTRIBUTE2,
ATTRIBUTE3,
ATTRIBUTE4,
ATTRIBUTE5,
CONTEXT,
EFFECTIVE_DATE_RULE_CODE,
JOURNAL_APPROVAL_FLAG,
LANGUAGE,
SOURCE_LANG,
IMPORT_USING_KEY_FLAG,
JE_SOURCE_KEY
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
from bec_ods_stg.gl_je_sources_tl
where kca_operation IN ('INSERT','UPDATE') and (JE_SOURCE_NAME, LANGUAGE,kca_seq_id) in (select JE_SOURCE_NAME, LANGUAGE,max(kca_seq_id) from bec_ods_stg.gl_je_sources_tl 
where kca_operation IN ('INSERT','UPDATE')
group by JE_SOURCE_NAME, LANGUAGE)
);

commit;

 
-- Soft Delete 
update bec_ods.gl_je_sources_tl set IS_DELETED_FLG = 'N';
commit;
update bec_ods.gl_je_sources_tl set IS_DELETED_FLG = 'Y'
where (JE_SOURCE_NAME, LANGUAGE)  in
(
select JE_SOURCE_NAME, LANGUAGE from bec_raw_dl_ext.gl_je_sources_tl
where (JE_SOURCE_NAME, LANGUAGE,KCA_SEQ_ID)
in 
(
select JE_SOURCE_NAME, LANGUAGE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.gl_je_sources_tl
group by JE_SOURCE_NAME, LANGUAGE
) 
and kca_operation= 'DELETE'
);
commit;
end;



update bec_etl_ctrl.batch_ods_info set last_refresh_date = getdate() where ods_table_name='gl_je_sources_tl';
commit;

