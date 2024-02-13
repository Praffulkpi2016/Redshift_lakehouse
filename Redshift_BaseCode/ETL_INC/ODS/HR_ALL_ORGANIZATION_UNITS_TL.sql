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

delete from bec_ods.HR_ALL_ORGANIZATION_UNITS_TL
where (ORGANIZATION_ID,LANGUAGE) in (
select stg.ORGANIZATION_ID,stg.LANGUAGE from bec_ods.HR_ALL_ORGANIZATION_UNITS_TL ods, bec_ods_stg.HR_ALL_ORGANIZATION_UNITS_TL stg
where ods.ORGANIZATION_ID = stg.ORGANIZATION_ID and ods.LANGUAGE = stg.LANGUAGE  and stg.kca_operation IN ('INSERT','UPDATE') );

commit;
-- Insert records	
insert into bec_ods.HR_ALL_ORGANIZATION_UNITS_TL
(
ORGANIZATION_ID
,LANGUAGE
,SOURCE_LANG
,NAME
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,CREATED_BY
,CREATION_DATE
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,IS_DELETED_FLG
,kca_seq_id,
	kca_seq_date
) 
SELECT ORGANIZATION_ID
,LANGUAGE
,SOURCE_LANG
,NAME
,LAST_UPDATE_DATE
,LAST_UPDATED_BY
,LAST_UPDATE_LOGIN
,CREATED_BY
,CREATION_DATE
,ZD_EDITION_NAME
,ZD_SYNC
,KCA_OPERATION
,'N' as IS_DELETED_FLG
,cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID ,
	kca_seq_date
FROM bec_ods_stg.HR_ALL_ORGANIZATION_UNITS_TL
where kca_operation IN ('INSERT','UPDATE') and (ORGANIZATION_ID,LANGUAGE,kca_seq_id) in (select ORGANIZATION_ID,LANGUAGE,max(kca_seq_id) from bec_ods_stg.HR_ALL_ORGANIZATION_UNITS_TL 
where kca_operation IN ('INSERT','UPDATE')
group by ORGANIZATION_ID,LANGUAGE);

commit;

 
-- Soft Delete 
update bec_ods.HR_ALL_ORGANIZATION_UNITS_TL set IS_DELETED_FLG = 'N';
commit;
update bec_ods.HR_ALL_ORGANIZATION_UNITS_TL set IS_DELETED_FLG = 'Y'
where (ORGANIZATION_ID,LANGUAGE)  in
(
select ORGANIZATION_ID,LANGUAGE from bec_raw_dl_ext.HR_ALL_ORGANIZATION_UNITS_TL
where (ORGANIZATION_ID,LANGUAGE,KCA_SEQ_ID)
in 
(
select ORGANIZATION_ID,LANGUAGE,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.HR_ALL_ORGANIZATION_UNITS_TL
group by ORGANIZATION_ID,LANGUAGE
) 
and kca_operation= 'DELETE'
);
commit;
end;

update bec_etl_ctrl.batch_ods_info 
set last_refresh_date = getdate()
where ods_table_name= 'hr_all_organization_units_tl';
commit;
