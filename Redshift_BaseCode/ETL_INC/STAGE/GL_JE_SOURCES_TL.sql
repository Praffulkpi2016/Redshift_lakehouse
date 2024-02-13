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
begin;

truncate
	table bec_ods_stg.gl_je_sources_tl;

insert
	into
	bec_ods_stg.gl_je_sources_tl
(JE_SOURCE_NAME,
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
,kca_operation
,kca_seq_id,
	kca_seq_date)
(
	select
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
,kca_operation
,kca_seq_id,
	kca_seq_date
	from
		bec_raw_dl_ext.gl_je_sources_tl
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' and (JE_SOURCE_NAME,LANGUAGE,kca_seq_id)
in  (select JE_SOURCE_NAME,LANGUAGE,max(kca_seq_id) from bec_raw_dl_ext.gl_je_sources_tl 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
group by JE_SOURCE_NAME,LANGUAGE) and
		( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'gl_je_sources_tl')
	 
            )
);
end;
