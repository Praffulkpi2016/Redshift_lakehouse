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

delete from bec_ods.WF_ACTIVITIES_TL
where (nvl(ITEM_TYPE, 'NA'),
	nvl(NAME, 'NA'),
	nvl(VERSION, 0),
	nvl(language, 'NA')) in (
select nvl(stg.ITEM_TYPE, 'NA') as ITEM_TYPE,
		nvl(stg.NAME, 'NA') as NAME,
		nvl(stg.VERSION, 0) as VERSION ,
		nvl(stg.language, 'NA') as language 	from bec_ods.WF_ACTIVITIES_TL ods, bec_ods_stg.WF_ACTIVITIES_TL stg
where nvl(ods.ITEM_TYPE, 'NA') = nvl(stg.ITEM_TYPE, 'NA') and
	  nvl(ods.NAME, 'NA') = nvl(stg.NAME, 'NA') and
	  nvl(ods.VERSION, 0) = nvl(stg.VERSION, 0) and 
	  nvl(ods.language, 'NA')=nvl(stg.language, 'NA') and 
	  stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.WF_ACTIVITIES_TL
       (	
	item_type,
	"name",
	version,
	display_name,
	"language",
	protect_level,
	custom_level,
	description,
	source_lang,
	security_group_id,
	zd_edition_name,
	zd_sync,
        KCA_OPERATION,
        IS_DELETED_FLG,
		kca_seq_id
		,kca_seq_date)	
(
	select
	item_type,
	"name",
	version,
	display_name,
	"language",
	protect_level,
	custom_level,
	description,
	source_lang,
	security_group_id,
	zd_edition_name,
	zd_sync,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		KCA_SEQ_DATE
	from bec_ods_stg.WF_ACTIVITIES_TL
	where kca_operation in ('INSERT','UPDATE') 
	and (nvl(ITEM_TYPE, 'NA'),
	nvl(NAME, 'NA'),
	nvl(VERSION, 0),nvl(language, 'NA'),kca_seq_id) in 
	(select nvl(ITEM_TYPE, 'NA') as ITEM_TYPE,
		nvl(NAME, 'NA') as NAME,
		nvl(VERSION, 0) as VERSION,
		nvl(language, 'NA') as language ,max(kca_seq_id) from bec_ods_stg.WF_ACTIVITIES_TL 
     where kca_operation in ('INSERT','UPDATE')
     group by nvl(ITEM_TYPE, 'NA'),
	nvl(NAME, 'NA'),
	nvl(VERSION, 0),
	nvl(language, 'NA'))
);

commit;



-- Soft delete
update bec_ods.WF_ACTIVITIES_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.WF_ACTIVITIES_TL set IS_DELETED_FLG = 'Y'
where (nvl(ITEM_TYPE, 'NA'),nvl(NAME, 'NA'),nvl(VERSION, 0),nvl(language, 'NA'))  in
(
select nvl(ITEM_TYPE, 'NA'),nvl(NAME, 'NA'),nvl(VERSION, 0),nvl(language, 'NA') from bec_raw_dl_ext.WF_ACTIVITIES_TL
where (nvl(ITEM_TYPE, 'NA'),nvl(NAME, 'NA'),nvl(VERSION, 0),nvl(language, 'NA'),KCA_SEQ_ID)
in 
(
select nvl(ITEM_TYPE, 'NA'),nvl(NAME, 'NA'),nvl(VERSION, 0),nvl(language, 'NA'),max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.WF_ACTIVITIES_TL
group by nvl(ITEM_TYPE, 'NA'),nvl(NAME, 'NA'),nvl(VERSION, 0),nvl(language, 'NA')
) 
and kca_operation= 'DELETE'
);
commit;;

end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'wf_activities_tl';

commit;