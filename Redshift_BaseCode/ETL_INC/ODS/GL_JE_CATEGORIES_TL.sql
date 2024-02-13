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

delete from bec_ods.gl_je_categories_tl
where (JE_CATEGORY_NAME,LANGUAGE) in (
select stg.JE_CATEGORY_NAME,stg.LANGUAGE from bec_ods.gl_je_categories_tl ods, bec_ods_stg.gl_je_categories_tl stg
where ods.JE_CATEGORY_NAME = stg.JE_CATEGORY_NAME 
and ods.LANGUAGE = stg.LANGUAGE 
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.gl_je_categories_tl
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
        IS_DELETED_FLG,
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
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.gl_je_categories_tl
	where kca_operation IN ('INSERT','UPDATE') 
	and (JE_CATEGORY_NAME,LANGUAGE,kca_seq_id) in 
	(select JE_CATEGORY_NAME,LANGUAGE,max(kca_seq_id) from bec_ods_stg.gl_je_categories_tl 
     where kca_operation IN ('INSERT','UPDATE')
     group by JE_CATEGORY_NAME,LANGUAGE)
);

commit;

 

-- Soft delete
update bec_ods.gl_je_categories_tl set IS_DELETED_FLG = 'N';
commit;
update bec_ods.gl_je_categories_tl set IS_DELETED_FLG = 'Y'
where (JE_CATEGORY_NAME,LANGUAGE)  in
(
select JE_CATEGORY_NAME,LANGUAGE from bec_raw_dl_ext.gl_je_categories_tl
where (JE_CATEGORY_NAME,LANGUAGE,KCA_SEQ_ID)
in 
(
select JE_CATEGORY_NAME,LANGUAGE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.gl_je_categories_tl
group by JE_CATEGORY_NAME,LANGUAGE
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'gl_je_categories_tl';

commit;