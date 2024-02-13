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

delete from bec_ods.fa_categories_tl
where (nvl(CATEGORY_ID,0),nvl(LANGUAGE,'NA'))  in (
select nvl(stg.CATEGORY_ID,0) as CATEGORY_ID,nvl(stg.LANGUAGE,'NA') as LANGUAGE  from bec_ods.fa_categories_tl ods, bec_ods_stg.fa_categories_tl stg
where nvl(ods.CATEGORY_ID,0) = nvl(stg.CATEGORY_ID,0)
and nvl(ods.LANGUAGE,'NA') = nvl(stg.LANGUAGE,'NA') 
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.fa_categories_tl
       (CATEGORY_ID,
		LANGUAGE,
		SOURCE_LANG,
		DESCRIPTION,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATED_BY,
		CREATION_DATE,
		LAST_UPDATE_LOGIN,
         KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id,
		kca_seq_date)	
(
	select
		CATEGORY_ID,
		LANGUAGE,
		SOURCE_LANG,
		DESCRIPTION,
		LAST_UPDATE_DATE,
		LAST_UPDATED_BY,
		CREATED_BY,
		CREATION_DATE,
		LAST_UPDATE_LOGIN,
         KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.fa_categories_tl
	where kca_operation IN ('INSERT','UPDATE') 
	and (nvl(CATEGORY_ID,0),nvl(LANGUAGE,'NA'),kca_seq_id) in 
	(select nvl(CATEGORY_ID,0) as CATEGORY_ID,nvl(LANGUAGE,'NA') as LANGUAGE,max(kca_seq_id) from bec_ods_stg.fa_categories_tl 
     where kca_operation IN ('INSERT','UPDATE')
     group by nvl(CATEGORY_ID,0),nvl(LANGUAGE,'NA'))
);

commit;

-- Soft delete
update bec_ods.fa_categories_tl set IS_DELETED_FLG = 'N';
commit;
update bec_ods.fa_categories_tl set IS_DELETED_FLG = 'Y'
where (nvl(CATEGORY_ID,0),nvl(LANGUAGE,'NA'))  in
(
select nvl(CATEGORY_ID,0),nvl(LANGUAGE,'NA') from bec_raw_dl_ext.fa_categories_tl
where (nvl(CATEGORY_ID,0),nvl(LANGUAGE,'NA'),KCA_SEQ_ID)
in 
(
select nvl(CATEGORY_ID,0),nvl(LANGUAGE,'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.fa_categories_tl
group by nvl(CATEGORY_ID,0),nvl(LANGUAGE,'NA')
) 
and kca_operation= 'DELETE'
);
commit;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'fa_categories_tl';

commit;