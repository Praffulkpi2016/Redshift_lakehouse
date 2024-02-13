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

delete from bec_ods.HR_LOCATIONS_ALL_TL
where (location_id,language) in (
select stg.location_id,stg.language 
from bec_ods.HR_LOCATIONS_ALL_TL ods, bec_ods_stg.HR_LOCATIONS_ALL_TL stg
where 
ods.location_id = stg.location_id and 
ods.language = stg.language 


and stg.kca_operation IN ('INSERT','UPDATE')
);


-- Insert records

insert into	bec_ods.HR_LOCATIONS_ALL_TL
       (
	location_id,
	"language",
	source_lang,
	location_code,
	description,
	last_update_date,
	last_updated_by,
	last_update_login,
	created_by,
	creation_date,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
	
	(
	select
		
	
     location_id,
	"language",
	source_lang,
	location_code,
	description,
	last_update_date,
	last_updated_by,
	last_update_login,
	created_by,
	creation_date,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
	from bec_ods_stg.HR_LOCATIONS_ALL_TL
	where kca_operation IN ('INSERT','UPDATE') 
	and (location_id,language ,kca_seq_id) in 
	(select location_id,language ,max(kca_seq_id) from bec_ods_stg.HR_LOCATIONS_ALL_TL 
     where kca_operation IN ('INSERT','UPDATE')
     group by location_id,language )
);
commit;

 
-- Soft delete
update bec_ods.HR_LOCATIONS_ALL_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.HR_LOCATIONS_ALL_TL set IS_DELETED_FLG = 'Y'
where (location_id,language)  in
(
select location_id,language from bec_raw_dl_ext.HR_LOCATIONS_ALL_TL
where (location_id,language,KCA_SEQ_ID)
in 
(
select location_id,language,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.HR_LOCATIONS_ALL_TL
group by location_id,language
) 
and kca_operation= 'DELETE'
);
commit;

end;


update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'hr_locations_all_tl';

commit;