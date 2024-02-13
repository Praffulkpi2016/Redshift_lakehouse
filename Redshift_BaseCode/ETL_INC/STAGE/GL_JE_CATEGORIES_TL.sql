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

truncate table bec_ods_stg.gl_je_categories_tl;

insert into	bec_ods_stg.gl_je_categories_tl
   (JE_CATEGORY_NAME,
	LANGUAGE,
	SOURCE_LANG,
	USER_JE_CATEGORY_NAME,
	LAST_UPDATE_DATE,
	LAST_UPDATED_BY,
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
	CONSOLIDATION_FLAG,
	JE_CATEGORY_KEY,
	ZD_EDITION_NAME,
	ZD_SYNC,
    KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(
	select
		JE_CATEGORY_NAME,
		LANGUAGE,
		SOURCE_LANG,
		USER_JE_CATEGORY_NAME,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
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
		CONSOLIDATION_FLAG,
		JE_CATEGORY_KEY,
		ZD_EDITION_NAME,
		ZD_SYNC,
        KCA_OPERATION,
		kca_seq_id,
	kca_seq_date
	from bec_raw_dl_ext.gl_je_categories_tl
	where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
	and (JE_CATEGORY_NAME,LANGUAGE,kca_seq_id) in 
	(select JE_CATEGORY_NAME,LANGUAGE,max(kca_seq_id) from bec_raw_dl_ext.gl_je_categories_tl 
     where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
     group by JE_CATEGORY_NAME,LANGUAGE)
        and	( kca_seq_date > (
		select
			(executebegints-prune_days)
		from
			bec_etl_ctrl.batch_ods_info
		where
			ods_table_name = 'gl_je_categories_tl')
		 
            )
);
end;