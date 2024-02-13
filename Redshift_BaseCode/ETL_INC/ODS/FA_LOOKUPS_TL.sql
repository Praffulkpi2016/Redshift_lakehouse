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

delete from bec_ods.FA_LOOKUPS_TL
where (nvl(LOOKUP_TYPE,'NA'),nvl(LOOKUP_CODE,'NA'),nvl(language,'NA')) in (
select nvl(stg.LOOKUP_TYPE,'NA') as LOOKUP_TYPE,nvl(stg.LOOKUP_CODE,'NA') as LOOKUP_CODE,nvl(stg.language,'NA') as language from bec_ods.FA_LOOKUPS_TL ods, bec_ods_stg.FA_LOOKUPS_TL stg
where nvl(ods.LOOKUP_TYPE,'NA') = nvl(stg.LOOKUP_TYPE,'NA') and nvl(ods.LOOKUP_CODE,'NA') = nvl(stg.LOOKUP_CODE,'NA')
and nvl(ods.language,'NA') = nvl(stg.language,'NA')
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.FA_LOOKUPS_TL
       (	
		LOOKUP_TYPE, 
		LOOKUP_CODE, 
		LANGUAGE, 
		SOURCE_LANG, 
		MEANING, 
		DESCRIPTION, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATE_LOGIN, 
		ZD_EDITION_NAME, 
		ZD_SYNC,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		LOOKUP_TYPE, 
		LOOKUP_CODE, 
		LANGUAGE, 
		SOURCE_LANG, 
		MEANING, 
		DESCRIPTION, 
		LAST_UPDATE_DATE, 
		LAST_UPDATED_BY, 
		CREATED_BY, 
		CREATION_DATE, 
		LAST_UPDATE_LOGIN, 
		ZD_EDITION_NAME, 
		ZD_SYNC,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.FA_LOOKUPS_TL
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(LOOKUP_TYPE,'NA'),nvl(LOOKUP_CODE,'NA'),nvl(language,'NA'),kca_seq_id) in 
	(select nvl(LOOKUP_TYPE,'NA') as LOOKUP_TYPE,nvl(LOOKUP_CODE,'NA') as LOOKUP_CODE,nvl(language,'NA') as language,max(kca_seq_id) as kca_seq_id from bec_ods_stg.FA_LOOKUPS_TL 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(LOOKUP_TYPE,'NA'),nvl(LOOKUP_CODE,'NA'),nvl(language,'NA'))
);

commit;

-- Soft delete
update bec_ods.FA_LOOKUPS_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.FA_LOOKUPS_TL set IS_DELETED_FLG = 'Y'
where (nvl(LOOKUP_TYPE,'NA'),nvl(LOOKUP_CODE,'NA'),nvl(language,'NA'))  in
(
select nvl(LOOKUP_TYPE,'NA'),nvl(LOOKUP_CODE,'NA'),nvl(language,'NA') from bec_raw_dl_ext.FA_LOOKUPS_TL
where (nvl(LOOKUP_TYPE,'NA'),nvl(LOOKUP_CODE,'NA'),nvl(language,'NA'),KCA_SEQ_ID)
in 
(
select nvl(LOOKUP_TYPE,'NA'),nvl(LOOKUP_CODE,'NA'),nvl(language,'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.FA_LOOKUPS_TL
group by nvl(LOOKUP_TYPE,'NA'),nvl(LOOKUP_CODE,'NA'),nvl(language,'NA')
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'fa_lookups_tl';

commit;